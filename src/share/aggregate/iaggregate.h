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

#ifndef OCEANBASE_SHARE_IAGGREGATE_H_
#define OCEANBASE_SHARE_IAGGREGATE_H_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/engine/expr/ob_expr.h"
// TODO: split this big headers
#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/vector/vector_basic_op.h"
#include "share/ob_define.h"
#include "share/aggregate/util.h"
#include "share/aggregate/agg_ctx.h"
#include "share/aggregate/aggr_extra.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "sql/engine/expr/ob_array_expr_utils.h"

namespace oceanbase
{
namespace share
{

namespace aggregate
{
using namespace sql;

int quick_add_batch_rows_for_count(IAggregate *agg, RuntimeContext &agg_ctx,
                                   const bool is_single_row_agg, const sql::ObBitVector &skip,
                                   const sql::EvalBound &bound, const RowSelector &row_sel,
                                   const int32_t agg_col_id, char *agg_cell);
namespace helper
{
// printer helper
void print_input_rows(const RowSelector &row_sel, const sql::ObBitVector &skip,
                      const sql::EvalBound &bound, const sql::ObAggrInfo &aggr_info,
                      bool is_first_row, sql::ObEvalCtx &ctx, IAggregate *agg, int64_t col_id);
inline bool has_extra_info(ObAggrInfo &info)
{
  bool has = false;
  switch (info.get_expr_type()) {
  case T_FUN_GROUP_CONCAT: {
    if (info.has_order_by_) {
      has = true;
    } else {
      has = false;
    }
    break;
  }
  case T_FUN_GROUP_RANK:
  case T_FUN_GROUP_DENSE_RANK:
  case T_FUN_GROUP_PERCENT_RANK:
  case T_FUN_GROUP_CUME_DIST:
  case T_FUN_MEDIAN:
  case T_FUN_GROUP_PERCENTILE_CONT:
  case T_FUN_GROUP_PERCENTILE_DISC:
  case T_FUN_KEEP_MAX:
  case T_FUN_KEEP_MIN:
  case T_FUN_KEEP_SUM:
  case T_FUN_KEEP_COUNT:
  case T_FUN_KEEP_WM_CONCAT:
  case T_FUN_WM_CONCAT:
  case T_FUN_PL_AGG_UDF:
  case T_FUN_JSON_ARRAYAGG:
  case T_FUN_ORA_JSON_ARRAYAGG:
  case T_FUN_JSON_OBJECTAGG:
  case T_FUN_ORA_JSON_OBJECTAGG:
  case T_FUN_ORA_XMLAGG:
  case T_FUNC_SYS_ARRAY_AGG:
  case T_FUN_HYBRID_HIST:
  case T_FUN_TOP_FRE_HIST:
  case T_FUN_AGG_UDF: {
    has = true;
    break;
  }
  default: {
    break;
  }
  }
  has = has || info.has_distinct_;
  return has;
}
} // end helper

template<ObExprOperatorType agg_func, VecValueTypeClass in_tc, VecValueTypeClass out_tc>
struct SingleRowAggregate;

template<typename Derived>
class BatchAggregateWrapper: public IAggregate
{
public:
  inline void set_inner_aggregate(IAggregate *agg) override
  {
    UNUSEDx(agg);
    return;
  }

  inline int init(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                  ObIAllocator &allocator) override
  {
    UNUSEDx(agg_ctx, agg_col_id, allocator);
    return OB_SUCCESS;
  }

  inline void destroy() override
  {
    return;
  }

  inline void reuse() override
  {
    return;
  }

  inline void* get_tmp_res(RuntimeContext &agg_ctx, int32_t agg_col_idx, char *agg_cell) override
  {
    return nullptr;
  }

  inline int64_t get_batch_calc_info(RuntimeContext &agg_ctx, int32_t agg_col_idx,
                                     char *agg_cell) override
  {
    UNUSEDx(agg_ctx, agg_col_idx, agg_cell);
    return 0;
  }

  int add_batch_rows(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                     const sql::ObBitVector &skip, const sql::EvalBound &bound, char *agg_cell,
                     const RowSelector row_sel = RowSelector{}) override
  {
    int ret = OB_SUCCESS;
    OB_ASSERT(agg_col_id < agg_ctx.aggr_infos_.count());
    Derived &derived = *static_cast<Derived *>(this);
    sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    ObIArray<ObExpr *> &param_exprs = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_;
#ifndef NDEBUG
    helper::print_input_rows(row_sel, skip, bound, agg_ctx.aggr_infos_.at(agg_col_id), false,
                             agg_ctx.eval_ctx_, this, agg_col_id);
#endif
    if (param_exprs.count() == 1
               || agg_ctx.aggr_infos_.at(agg_col_id).is_implicit_first_aggr()) { // by pass implicit first row
      VectorFormat fmt = VEC_INVALID;
      ObExpr *param_expr = nullptr;
      // by pass implicit first row
      // set param_expr to aggr_info.expr_
      if (agg_ctx.aggr_infos_.at(agg_col_id).is_implicit_first_aggr()) {
        fmt = agg_ctx.aggr_infos_.at(agg_col_id).expr_->get_format(ctx);
        param_expr = agg_ctx.aggr_infos_.at(agg_col_id).expr_;
      } else {
        fmt = param_exprs.at(0)->get_format(ctx);
        param_expr = param_exprs.at(0);
      }
      switch (fmt) {
      case common::VEC_UNIFORM: {
        ret = add_batch_rows<uniform_fmt<Derived::IN_TC, false>>(
          agg_ctx, skip, bound, *param_expr, agg_col_id, agg_cell, row_sel);
        break;
      }
      case common::VEC_UNIFORM_CONST: {
        ret = add_batch_rows<uniform_fmt<Derived::IN_TC, true>>(
          agg_ctx, skip, bound, *param_expr, agg_col_id, agg_cell, row_sel);
        break;
      }
      case common::VEC_FIXED: {
        ret = add_batch_rows<fixlen_fmt<Derived::IN_TC>>(
          agg_ctx, skip, bound, *param_expr, agg_col_id, agg_cell, row_sel);
        break;
      }
      case common::VEC_DISCRETE: {
        ret = add_batch_rows<discrete_fmt<Derived::IN_TC>>(
          agg_ctx, skip, bound, *param_expr, agg_col_id, agg_cell, row_sel);
        break;
      }
      case common::VEC_CONTINUOUS: {
        ret = add_batch_rows<continuous_fmt<Derived::IN_TC>>(
          agg_ctx, skip, bound, *param_expr, agg_col_id, agg_cell, row_sel);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected fmt", K(fmt), K(*param_exprs.at(0)));
      }
      }
      if (OB_FAIL(ret)) {
        SQL_LOG(WARN, "add batch rows failed", K(ret), K(fmt), K(param_exprs.at(0)->datum_meta_));
      }
    } else if (param_exprs.empty() && !agg_ctx.aggr_infos_.at(agg_col_id).is_implicit_first_aggr()) {
      ObItemType agg_fun_type = agg_ctx.aggr_infos_.at(agg_col_id).get_expr_type();
      if (T_FUN_COUNT == agg_fun_type) {
        if (OB_FAIL(quick_add_batch_rows_for_count(
              this, agg_ctx,
              (std::is_same<Derived, SingleRowAggregate<T_FUN_COUNT, VEC_TC_INTEGER, VEC_TC_INTEGER>>::value
              || std::is_same<Derived, SingleRowAggregate<T_FUN_COUNT, VEC_TC_INTEGER, VEC_TC_NUMBER>>::value),
              skip, bound, row_sel, agg_col_id, agg_cell))) {
          SQL_LOG(WARN, "quick add batch rows failed", K(ret));
        }
      } else if (T_FUN_GROUP_ID == agg_fun_type) {
        // TODO:
      } else {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected aggregate function", K(ret), K(agg_fun_type),
                K(*agg_ctx.aggr_infos_.at(agg_col_id).expr_));
      }
    } else if (defined_add_param_batch<Derived>::value) {
      if (OB_FAIL(add_params_batch_row(agg_ctx, agg_col_id, skip, bound, row_sel, agg_cell))) {
        SQL_LOG(WARN, "add param batch rows failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "should not arrive here", K(ret));
    }
    return ret;
  }

  inline int get_nested_expr_vec(RuntimeContext &agg_ctx, const ObExpr *param_expr, ObIVector *&param_vec)
  {
    int ret = OB_SUCCESS;
    ObEvalCtx &eval_ctx = agg_ctx.eval_ctx_;
    param_vec = param_expr->get_vector(eval_ctx);
    VectorFormat fmt = param_expr->get_format(eval_ctx);
    if (param_expr->is_nested_expr()) {
      if (param_expr->attrs_cnt_ != 3) { // only vector type supported
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected attrs_cnt_", K(param_expr->attrs_cnt_));
      } else if (fmt == common::VEC_DISCRETE || fmt == common::VEC_CONTINUOUS) {
        param_vec = param_expr->attrs_[2]->get_vector(eval_ctx);
      }
    }
    return ret;
  }

  inline int add_batch_for_multi_groups(RuntimeContext &agg_ctx, AggrRowPtr *agg_rows,
                                        RowSelector &row_sel, const int64_t batch_size,
                                        const int32_t agg_col_id) override
  {
#define INNER_ADD(vec_tc)                                                                          \
  case (vec_tc): {                                                                                 \
    ret = inner_add_for_multi_groups<ObFixedLengthFormat<RTCType<vec_tc>>>(                        \
      agg_ctx, agg_rows, row_sel, batch_size, agg_col_id, param_expr);                             \
  } break

    int ret = OB_SUCCESS;
    ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_id);
    ObEvalCtx &eval_ctx = agg_ctx.eval_ctx_;
    VectorFormat fmt = VEC_INVALID;
    ObExpr *param_expr = nullptr;
    Derived *derived_this = static_cast<Derived *>(this);
#ifndef NDEBUG
    int64_t mock_skip_data = 0;
    ObBitVector *mock_skip = to_bit_vector(&mock_skip_data);
    helper::print_input_rows(row_sel, *mock_skip, sql::EvalBound(), aggr_info,
                             aggr_info.is_implicit_first_aggr(), eval_ctx, this, agg_col_id);
#endif
    if (aggr_info.is_implicit_first_aggr()) {
      fmt = aggr_info.expr_->get_format(eval_ctx);
      param_expr = aggr_info.expr_;
    } else if (aggr_info.param_exprs_.count() == 1) {
      param_expr = aggr_info.param_exprs_.at(0);
      fmt = param_expr->get_format(eval_ctx);
    }
    if (OB_ISNULL(param_expr)) { // count(*)
      for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
        int batch_idx = row_sel.index(i);
        char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_rows[batch_idx]);
        if (OB_FAIL(derived_this->add_one_row(agg_ctx, batch_idx, batch_size, false, nullptr, 0,
                                              agg_col_id, agg_cell))) {
          SQL_LOG(WARN, "inner add one row failed", K(ret));
        }
      }
    } else {
      VecValueTypeClass vec_tc = param_expr->get_vec_value_tc();
      switch(fmt) {
      case common::VEC_UNIFORM: {
        ret = inner_add_for_multi_groups<uniform_fmt<Derived::IN_TC, false>>(
          agg_ctx, agg_rows, row_sel, batch_size, agg_col_id, param_expr);
        break;
      }
      case common::VEC_UNIFORM_CONST: {
        ret = inner_add_for_multi_groups<uniform_fmt<Derived::IN_TC, true>>(
          agg_ctx, agg_rows, row_sel, batch_size, agg_col_id, param_expr);
        break;
      }
      case common::VEC_DISCRETE: {
        ret = inner_add_for_multi_groups<discrete_fmt<Derived::IN_TC>>(
          agg_ctx, agg_rows, row_sel, batch_size, agg_col_id, param_expr);
        break;
      }
      case common::VEC_CONTINUOUS: {
        ret = inner_add_for_multi_groups<continuous_fmt<Derived::IN_TC>>(
          agg_ctx, agg_rows, row_sel, batch_size, agg_col_id, param_expr);
        break;
      }
      case common::VEC_FIXED: {
        switch(vec_tc) {
          LST_DO_CODE(INNER_ADD, AGG_VEC_TC_LIST);
        default: {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "unexpected type class", K(vec_tc));
        }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      }
    }
    return ret;
#undef INNER_ADD
  }

  int collect_batch_group_results(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                                  const int32_t cur_group_id, const int32_t output_start_idx,
                                  const int32_t expect_batch_size, int32_t &output_size,
                                  const ObBitVector *skip = nullptr,
                                  const bool init_vector = true) override
  {
    int ret = OB_SUCCESS;
    OB_ASSERT(agg_col_id < agg_ctx.aggr_infos_.count());
    OB_ASSERT(agg_ctx.aggr_infos_.at(agg_col_id).expr_ != NULL);

    ObExpr &agg_expr = *agg_ctx.aggr_infos_.at(agg_col_id).expr_;
    int32_t loop_cnt =
      min(expect_batch_size, static_cast<int32_t>(agg_ctx.agg_rows_.count()) - cur_group_id);
    if (loop_cnt <= 0) {
      SQL_LOG(DEBUG, "no need to collect", K(ret), K(agg_ctx.agg_rows_.count()), K(cur_group_id));
    } else {
      output_size = 0;
      if (init_vector && OB_FAIL(agg_expr.init_vector_for_write(
            agg_ctx.eval_ctx_, agg_expr.get_default_res_format(), expect_batch_size))) {
        SQL_LOG(WARN, "init vector for write failed", K(ret));
      } else {
        VectorFormat res_vec = agg_expr.get_format(agg_ctx.eval_ctx_);
        VecValueTypeClass res_tc = agg_expr.get_vec_value_tc();
        switch (res_vec) {
        case common::VEC_FIXED: {
          ret = collect_group_results<fixlen_fmt<Derived::OUT_TC>>(
            agg_ctx, agg_col_id, cur_group_id, output_start_idx, loop_cnt, skip);
          break;
        }
        case common::VEC_DISCRETE: {
          ret = collect_group_results<discrete_fmt<Derived::OUT_TC>>(
            agg_ctx, agg_col_id, cur_group_id, output_start_idx, loop_cnt, skip);
          break;
        }
        case common::VEC_UNIFORM_CONST: {
          // must be null
          ret = ret = collect_group_results<uniform_fmt<VEC_TC_NULL, true>>(
            agg_ctx, agg_col_id, cur_group_id, output_start_idx, loop_cnt, skip);
          break;
        }
        case common::VEC_UNIFORM: {
          // must be null
          ret = collect_group_results<uniform_fmt<VEC_TC_NULL, false>>(
            agg_ctx, agg_col_id, cur_group_id, output_start_idx, loop_cnt, skip);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "invalid result format", K(ret), K(res_vec), K(agg_col_id));
        }
        }
        if (OB_FAIL(ret)) {
          SQL_LOG(WARN, "collect batch group results failed", K(ret), K(res_vec));
        } else {
          output_size = loop_cnt;
        }
      }
    }
    return ret;
  }

  int eval_group_extra_result(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                              const int32_t cur_group_id) override
  {
    int ret = OB_NOT_IMPLEMENT;
    SQL_LOG(WARN, "not implemented", K(ret), K(*this));
    return ret;
  }

  int collect_batch_group_results(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                                  const int32_t output_start_idx, const int32_t batch_size,
                                  const ObCompactRow **rows, const RowMeta &row_meta,
                                  const int32_t row_start_idx = 0, const bool need_init_vector = true) override
  {
    int ret = OB_SUCCESS;
    OB_ASSERT(agg_col_id < agg_ctx.aggr_infos_.count());
    OB_ASSERT(agg_ctx.aggr_infos_.at(agg_col_id).expr_ != NULL);

    ObExpr &agg_expr = *agg_ctx.aggr_infos_.at(agg_col_id).expr_;
    if (OB_LIKELY(batch_size > 0)) {
      if (need_init_vector && OB_FAIL(agg_expr.init_vector_for_write(
            agg_ctx.eval_ctx_, agg_expr.get_default_res_format(), batch_size))) {
        SQL_LOG(WARN, "init vector for write failed", K(ret));
      } else {
        VectorFormat res_fmt = agg_expr.get_format(agg_ctx.eval_ctx_);
        VecValueTypeClass res_tc = get_vec_value_tc(
          agg_expr.datum_meta_.type_, agg_expr.datum_meta_.scale_, agg_expr.datum_meta_.precision_);
        switch (res_fmt) {
        case VEC_FIXED: {
          ret = collect_group_results<fixlen_fmt<Derived::OUT_TC>>(
            agg_ctx, agg_col_id, output_start_idx, batch_size, rows, row_meta, row_start_idx, need_init_vector);
          break;
        }
        case VEC_DISCRETE: {
          ret = collect_group_results<discrete_fmt<Derived::OUT_TC>>(
            agg_ctx, agg_col_id, output_start_idx, batch_size, rows, row_meta, row_start_idx, need_init_vector);
          break;
        }
        case VEC_UNIFORM_CONST: {
          // must be null type
          ret = collect_group_results<uniform_fmt<VEC_TC_NULL, true>>(
            agg_ctx, agg_col_id, output_start_idx, batch_size, rows, row_meta, row_start_idx, need_init_vector);
          break;
        }
        case VEC_UNIFORM: {
          // must be null type
          ret = collect_group_results<uniform_fmt<VEC_TC_NULL, false>>(
            agg_ctx, agg_col_id, output_start_idx, batch_size, rows, row_meta, row_start_idx, need_init_vector);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "invalid result format", K(ret), K(res_fmt), K(agg_col_id));
        }
        }
        if (OB_FAIL(ret)) { SQL_LOG(WARN, "collect batch group results failed", K(ret)); }
      }
    }
    return ret;
  }

protected:
  template <typename ColumnFmt>
  int inner_add_for_multi_groups(RuntimeContext &agg_ctx, AggrRowPtr *agg_rows, RowSelector &row_sel,
                                 const int64_t batch_size, const int32_t agg_col_id,
                                 ObExpr *param_expr)
  {
    int ret = OB_SUCCESS;
    ObIVector *ivec = param_expr->get_vector(agg_ctx.eval_ctx_);
    ColumnFmt *param_vec = static_cast<ColumnFmt *>(ivec);
    VectorFormat fmt = ivec->get_format();
    ObArenaAllocator tmp_allocator;
    bool is_null = false;
    const char *payload = nullptr;
    int32_t len = 0;
    Derived *derived_this = static_cast<Derived *>(this);
    for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
      int64_t batch_idx = row_sel.index(i);
      if (param_expr->is_nested_expr() && !is_uniform_format(ivec->get_format())) {
        payload = nullptr;
        if (OB_FAIL(ObArrayExprUtils::get_collection_payload(tmp_allocator, agg_ctx.eval_ctx_, *param_expr, batch_idx, payload, len))) {
          SQL_LOG(WARN, "get nested collection payload failed", K(ret));
        } else {
          is_null = (payload == nullptr);
        }
      } else {
        param_vec->get_payload(batch_idx, is_null, payload, len);
      }
      char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_rows[batch_idx]);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(derived_this->add_one_row(agg_ctx, batch_idx, batch_size, is_null, payload, len,
                                            agg_col_id, agg_cell))) {
        SQL_LOG(WARN, "inner add one row failed", K(ret));
      }
    }
    return ret;
  }
  template <>
  int inner_add_for_multi_groups<ObVectorBase>(RuntimeContext &agg_ctx, AggrRowPtr *agg_rows,
                                               RowSelector &row_sel, const int64_t batch_size,
                                               const int32_t agg_col_id, ObExpr *param_expr)
  {
    return OB_NOT_IMPLEMENT;
  }
  template <typename ColumnFmt>
  int add_batch_rows(RuntimeContext &agg_ctx, const sql::ObBitVector &skip,
                     const sql::EvalBound &bound, const ObExpr &param_expr,
                     const int32_t agg_col_id, char *agg_cell, const RowSelector row_sel)
  {
    int ret = OB_SUCCESS;
    ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    ColumnFmt *columns = nullptr;
    ObIVector *ivec = nullptr;
    bool all_not_null = false;
    Derived &derived = *static_cast<Derived *>(this);
    void *tmp_res = derived.get_tmp_res(agg_ctx, agg_col_id, agg_cell);
    int64_t calc_info = derived.get_batch_calc_info(agg_ctx, agg_col_id, agg_cell);
    if (OB_FAIL(get_nested_expr_vec(agg_ctx, &param_expr, ivec))) {
      SQL_LOG(WARN, "get nested expr vec failed", K(ret));
    } else if (FALSE_IT(columns = static_cast<ColumnFmt *>(ivec))) {
    } else if (FALSE_IT(all_not_null = !columns->has_null())) {
    } else if (OB_LIKELY(!agg_ctx.removal_info_.enable_removal_opt_)) {
      if (OB_LIKELY(row_sel.is_empty() && bound.get_all_rows_active())) {
        if (all_not_null) {
          for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
            if (OB_FAIL(AddRow<Derived>::do_op(derived, agg_ctx, *columns, i, agg_col_id,
                                                     agg_cell, tmp_res, calc_info))) {
              SQL_LOG(WARN, "add row failed", K(ret));
            }
          } // end for
          if (agg_cell != nullptr) {
            NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
            not_nulls.set(agg_col_id);
          }
        } else {
          for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
            if (OB_FAIL(AddNullableRow<Derived>::do_op(
                  derived, agg_ctx, *columns, i, agg_col_id, agg_cell, tmp_res, calc_info))) {
              SQL_LOG(WARN, "add row failed", K(ret));
            }
          } // end for
        }
      } else if (!row_sel.is_empty()) {
        if (all_not_null) {
          for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
            if (OB_FAIL(AddRow<Derived>::do_op(derived, agg_ctx, *columns, row_sel.index(i),
                                                     agg_col_id, agg_cell, tmp_res, calc_info))) {
              SQL_LOG(WARN, "add row failed", K(ret));
            }
          } // end for
          if (agg_cell != nullptr) {
            NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
            not_nulls.set(agg_col_id);
          }
        } else {
          for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
            if (OB_FAIL(AddNullableRow<Derived>::do_op(
                  derived, agg_ctx, *columns, row_sel.index(i), agg_col_id, agg_cell, tmp_res,
                  calc_info))) {
              SQL_LOG(WARN, "add row failed", K(ret));
            }
          } // end for
        }
      } else {
        if (all_not_null) {
          for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
            if (skip.at(i)) {
            } else if (OB_FAIL(AddRow<Derived>::do_op(derived, agg_ctx, *columns, i,
                                                            agg_col_id, agg_cell, tmp_res,
                                                            calc_info))) {
              SQL_LOG(WARN, "add row failed", K(ret));
            }
          } // end for
          if (agg_cell != nullptr) {
            NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
            not_nulls.set(agg_col_id);
          }
        } else {
          for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
            if (skip.at(i)) {
            } else if (OB_FAIL(AddNullableRow<Derived>::do_op(
                         derived, agg_ctx, *columns, i, agg_col_id, agg_cell, tmp_res,
                         calc_info))) {
              SQL_LOG(WARN, "add row failed", K(ret));
            }
          } // end for
        }
      }
    } else {
      if (row_sel.is_empty()) {
        if (bound.get_all_rows_active()) {
          for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
            ret = removal_opt<Derived>::add_or_sub_row(derived, agg_ctx, *columns, i, agg_col_id, agg_cell,
                                                       tmp_res, calc_info);
            if (OB_FAIL(ret)) { SQL_LOG(WARN, "add or sub row failed", K(ret)); }
          }
        } else {
          for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
            if (skip.at(i)) {
            } else {
              ret = removal_opt<Derived>::add_or_sub_row(derived, agg_ctx, *columns, i, agg_col_id, agg_cell,
                                                         tmp_res, calc_info);
              if (OB_FAIL(ret)) { SQL_LOG(WARN, "add or sub row failed", K(ret)); }
            }
          }
        }
      } else {
        for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
          ret = removal_opt<Derived>::add_or_sub_row(derived, agg_ctx, *columns, i, agg_col_id, agg_cell,
                                                     tmp_res, calc_info);
          if (OB_FAIL(ret)) { SQL_LOG(WARN, "add or sub row failed", K(ret)); }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(
            collect_tmp_result<Derived>::do_op(derived, agg_ctx, agg_col_id, agg_cell))) {
        SQL_LOG(WARN, "collect tmp result failed", K(ret));
      }
    }
    return ret;
  }

  template<>
  int add_batch_rows<ObVectorBase>(RuntimeContext &agg_ctx, const sql::ObBitVector &skip,
                     const sql::EvalBound &bound, const ObExpr &param_expr,
                     const int32_t agg_col_id, char *agg_cell,
                     const RowSelector row_sel)
  {
    int ret = OB_NOT_IMPLEMENT;
    SQL_LOG(WARN, "not implemented", K(ret), K(*this));
    return ret;
  }

  template <typename ResultFmt>
  int collect_group_results(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                            const int32_t start_gid, const int32_t start_output_idx,
                            const int32_t batch_size, const ObBitVector *skip)
  {
    int ret = OB_SUCCESS;
    OB_ASSERT(agg_col_id < agg_ctx.aggr_infos_.count());
    OB_ASSERT(agg_ctx.aggr_infos_.at(agg_col_id).expr_ != NULL);
    ObExpr *agg_expr = agg_ctx.aggr_infos_.at(agg_col_id).expr_;
    ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_id);
    ObEvalCtx::BatchInfoScopeGuard guard(agg_ctx.eval_ctx_);

    const char *agg_cell = nullptr;
    int32_t agg_cell_len = 0;
    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      if (skip != nullptr && skip->at(start_output_idx + i)) { continue; }
      guard.set_batch_idx(start_output_idx + i);
      agg_cell = nullptr;
      agg_cell_len = 0;
      const char* agg_row = agg_ctx.agg_rows_.at(start_gid + i);
      agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_row);
      if (OB_FAIL(ret)) {
      } else if (FALSE_IT(agg_cell_len = agg_ctx.row_meta().get_cell_len(agg_col_id, agg_row))) {
      } else if (OB_FAIL(static_cast<Derived *>(this)->template collect_group_result<ResultFmt>(
                   agg_ctx, *agg_expr, agg_col_id, agg_cell, agg_cell_len))) {
        SQL_LOG(WARN, "collect group result failed", K(ret));
      }
    }
    return ret;
  }

  template <>
  int collect_group_results<ObVectorBase>(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                                          const int32_t start_gid, const int32_t start_output_idx,
                                          const int32_t batch_size, const ObBitVector *skip)
  {
    int ret = OB_NOT_IMPLEMENT;
    SQL_LOG(WARN, "not implemented", K(ret), K(*this));
    return ret;
  }

  template <typename ResultFmt>
  int collect_group_results(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                            const int32_t output_start_idx, const int32_t batch_size,
                            const ObCompactRow **rows, const RowMeta &row_meta,
                            const int32_t row_start_idx, const bool need_init_vector)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(rows)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid null rows", K(ret));
    } else {
      ObEvalCtx::BatchInfoScopeGuard batch_guard(agg_ctx.eval_ctx_);
      ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_id);
      ObExpr *agg_expr = agg_ctx.aggr_infos_.at(agg_col_id).expr_;
      const char *agg_cell = nullptr;
      int32_t agg_cell_len = 0;
      if (need_init_vector && OB_FAIL(agg_expr->init_vector_for_write(
            agg_ctx.eval_ctx_, agg_expr->get_default_res_format(), batch_size))) {
        SQL_LOG(WARN, "init vector for write failed", K(ret));
      }
      for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
        batch_guard.set_batch_idx(output_start_idx + i);
        const ObCompactRow *row = rows[row_start_idx + i];
        if (OB_ISNULL(row)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "invalid compact row", K(ret), K(row_start_idx), K(i));
        } else {
          const char *agg_row = static_cast<const char *>(row->get_extra_payload(row_meta));
          agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_row);
          if (OB_FAIL(ret)) {
          } else {
            agg_cell_len = agg_ctx.row_meta().get_cell_len(agg_col_id, agg_row);
            SQL_LOG(DEBUG, "collect group results", K(agg_col_id), K(output_start_idx), K(i),
                    K(batch_size), K(row_start_idx),
                    K(agg_ctx.aggr_infos_.at(agg_col_id).get_expr_type()), KP(agg_cell),
                    K(agg_cell_len), K(agg_cell), K(row_meta), KP(row), KP(agg_expr),
                    KPC(agg_expr));
            if (OB_FAIL(static_cast<Derived *>(this)->template collect_group_result<ResultFmt>(
                  agg_ctx, *agg_expr, agg_col_id, agg_cell, agg_cell_len))) {
              SQL_LOG(WARN, "collect group result failed", K(ret));
            }
          }
        }
      }
    }
    return ret;
  }
  template <>
  int collect_group_results<ObVectorBase>(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                                          const int32_t output_start_idx, const int32_t batch_size,
                                          const ObCompactRow **rows, const RowMeta &row_meta,
                                          const int32_t row_start_idx, const bool need_init_vector)
  {
    int ret = OB_NOT_IMPLEMENT;
    SQL_LOG(WARN, "not implemented", K(ret));
    return ret;
  }

  int add_params_batch_row(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                           const sql::ObBitVector &skip, const sql::EvalBound &bound,
                           const RowSelector &row_sel, char *aggr_cell)
  {
#define ADD_COLUMN_ROWS(vec_tc)                                                                    \
  case (vec_tc): {                                                                                 \
    ret = add_param_batch<Derived>::do_op(                                                         \
      derived, agg_ctx, skip, *tmp_skip, bound, row_sel, agg_col_id, param_id,                     \
      *static_cast<ObFixedLengthFormat<RTCType<vec_tc>> *>(param_vec), aggr_cell);                 \
  } break
    int ret = OB_SUCCESS;
    ObEvalCtx::TempAllocGuard alloc_guard(agg_ctx.eval_ctx_);
    ObIArray<ObExpr *> &param_exprs = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_;
    int64_t skip_size = (bound.batch_size() / (sizeof(uint64_t) * CHAR_BIT) + 1) * sizeof(uint64_t);
    char *skip_buf = nullptr;
    if (OB_ISNULL(skip_buf = (char *)alloc_guard.get_allocator().alloc(skip_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      MEMSET(skip_buf, 0, skip_size);
      ObBitVector *tmp_skip = to_bit_vector(skip_buf);
      tmp_skip->deep_copy(skip, bound.batch_size());
      Derived &derived = *static_cast<Derived *>(this);
      for (int param_id = 0; OB_SUCC(ret) && param_id < param_exprs.count(); param_id++) {
        ObIVector *param_vec = param_exprs.at(param_id)->get_vector(agg_ctx.eval_ctx_);
        VectorFormat param_fmt = param_exprs.at(param_id)->get_format(agg_ctx.eval_ctx_);
        VecValueTypeClass param_tc = get_vec_value_tc(
          param_exprs.at(param_id)->datum_meta_.type_, param_exprs.at(param_id)->datum_meta_.scale_,
          param_exprs.at(param_id)->datum_meta_.precision_);
        switch (param_fmt) {
        case common::VEC_UNIFORM_CONST: {
          ret = add_param_batch<Derived>::do_op(
            derived, agg_ctx, skip, *tmp_skip, bound, row_sel, agg_col_id, param_id,
            *static_cast<ObUniformFormat<true> *>(param_vec), aggr_cell);
          break;
        }
        case common::VEC_UNIFORM: {
          ret = add_param_batch<Derived>::do_op(
            derived, agg_ctx, skip, *tmp_skip, bound, row_sel, agg_col_id, param_id,
            *static_cast<ObUniformFormat<false> *>(param_vec), aggr_cell);
          break;
        }
        case common::VEC_DISCRETE: {
          ret = add_param_batch<Derived>::do_op(
            derived, agg_ctx, skip, *tmp_skip, bound, row_sel, agg_col_id, param_id,
            *static_cast<ObDiscreteFormat *>(param_vec), aggr_cell);
          break;
        }
        case common::VEC_CONTINUOUS: {
          ret = add_param_batch<Derived>::do_op(
            derived, agg_ctx, skip, *tmp_skip, bound, row_sel, agg_col_id, param_id,
            *static_cast<ObContinuousFormat *>(param_vec), aggr_cell);
          break;
        }
        case common::VEC_FIXED: {
          switch (param_tc) {
            LST_DO_CODE(ADD_COLUMN_ROWS, AGG_FIXED_TC_LIST);
          default: {
            ret = OB_ERR_UNDEFINED;
            SQL_LOG(WARN, "invalid param type class", K(ret), K(param_tc));
          }
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "invalid param format", K(ret), K(param_fmt), K(param_tc));
          break;
        }
        }
      }
    }
    return ret;
#undef ADD_COLUMN_ROWS
  }

};

template<typename Aggregate>
class DistinctWrapper final: public BatchAggregateWrapper<DistinctWrapper<Aggregate>>
{
  using BaseClass = BatchAggregateWrapper<DistinctWrapper<Aggregate>>;
public:
  static const VecValueTypeClass IN_TC = Aggregate::IN_TC;
  static const VecValueTypeClass OUT_TC = Aggregate::OUT_TC;
public:
  DistinctWrapper(): agg_(nullptr) {}

  inline void set_inner_aggregate(IAggregate *agg) override {
    agg_ = agg;
  }

  int init(RuntimeContext &agg_ctx, const int64_t agg_col_id, ObIAllocator &allocator) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(agg_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid null aggregate", K(ret));
    } else if (OB_FAIL(agg_->init(agg_ctx, agg_col_id, allocator))) {
      SQL_LOG(WARN, "init aggregate failed", K(ret));
    }
    return ret;
  }

  int add_batch_rows(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                     const sql::ObBitVector &skip, const sql::EvalBound &bound, char *agg_cell,
                     const RowSelector row_sel = RowSelector{}) override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(agg_cell);
    OB_ASSERT(agg_ != NULL);
    OB_ASSERT(agg_col_id < agg_ctx.aggr_infos_.count());
    ObIArray<ObExpr *> &param_exprs = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_;
    HashBasedDistinctVecExtraResult *extra = agg_ctx.get_distinct_store(agg_col_id, agg_cell);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(extra) || !extra->is_inited()) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid null extra or extra is not inited", K(ret), K(agg_col_id), KP(extra));
    } else if (OB_FAIL(extra->insert_row_for_batch(param_exprs, bound.end(), &skip,
                                                   bound.start()))) {
      SQL_LOG(WARN, "add batch rows failed", K(ret));
    }
    return ret;
  }

  int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx, AggrRowPtr group_row,
                         AggrRowPtr rollup_row, int64_t cur_rollup_group_idx,
                         int64_t max_group_cnt = INT64_MIN) override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(cur_rollup_group_idx, max_group_cnt);
    sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    ObIArray<ObExpr *> &param_exprs = agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_;
    char *curr_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, group_row);
    char *rollup_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, rollup_row);
    HashBasedDistinctVecExtraResult *ad_result =
      agg_ctx.get_distinct_store(agg_col_idx, curr_agg_cell);
    HashBasedDistinctVecExtraResult *rollup_result =
      agg_ctx.get_distinct_store(agg_col_idx, rollup_agg_cell);
    if (OB_ISNULL(ad_result) || !ad_result->is_inited() || OB_ISNULL(rollup_result)
        || !rollup_result->is_inited()) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "distinct set is NULL", K(ret));
    } else if (OB_FAIL(ad_result->init_vector_default(ctx, ctx.max_batch_size_))) {
      SQL_LOG(WARN, "failed to init vector default", K(ret));
    } else if (OB_FAIL(ad_result->brs_holder_.save(ctx.max_batch_size_))) {
      SQL_LOG(WARN, "backup datum failed", K(ret));
    } else {
      int64_t read_rows = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(ad_result->get_next_unique_hash_table_batch(param_exprs, ctx.max_batch_size_,
                                                                read_rows))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            SQL_LOG(WARN, "get row from distinct set failed", K(ret));
          }
          break;
        } else if (OB_FAIL(rollup_result->insert_row_for_batch(param_exprs, read_rows))) {
          SQL_LOG(WARN, "add_row failed", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ad_result->brs_holder_.restore())) {
      SQL_LOG(WARN, "restore datum failed", K(ret));
    }
    return ret;
  }

  int eval_group_extra_result(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                              const int32_t group_id) override
  {
    int ret = OB_SUCCESS;
    OB_ASSERT(agg_col_id < agg_ctx.aggr_infos_.count());
    OB_ASSERT(agg_ctx.aggr_infos_.at(agg_col_id).expr_ != NULL);
    ObExpr *agg_expr = agg_ctx.aggr_infos_.at(agg_col_id).expr_;
    const char *agg_cell = nullptr;
    int32_t agg_cell_len = 0;
    agg_ctx.get_agg_payload(agg_col_id, group_id, agg_cell, agg_cell_len);
    OB_ASSERT(agg_ != NULL);
    sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    HashBasedDistinctVecExtraResult *ad_result = agg_ctx.get_distinct_store(agg_col_id, agg_cell);
    ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (OB_ISNULL(ad_result) || !ad_result->is_inited()) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid null extra", K(ret));
    } else if (OB_FAIL(ad_result->init_vector_default(ctx, ctx.max_batch_size_))) {
      SQL_LOG(WARN, "failed to init vector default", K(ret));
    } else if (OB_FAIL(ad_result->brs_holder_.save(ctx.max_batch_size_))) {
      SQL_LOG(WARN, "backup datum failed", K(ret));
    } else if (agg_ctx.has_rollup_ && group_id > 0) {
      if (group_id > agg_ctx.rollup_context_->start_partial_rollup_idx_
          && group_id <= agg_ctx.rollup_context_->end_partial_rollup_idx_) {
        // Group id greater than zero in sort based group by must be rollup,
        // distinct set is sorted and iterated in rollup_process(), rewind here.
        if (OB_FAIL(ad_result->rewind())) {
          SQL_LOG(WARN, "rewind iterator failed", K(ret));
        }
        SQL_LOG(DEBUG, "debug process distinct batch", K(group_id),
                K(agg_ctx.rollup_context_->start_partial_rollup_idx_),
                K(agg_ctx.rollup_context_->end_partial_rollup_idx_));
      }
    }
    char *skip_mem = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(skip_mem = (char *)alloc_guard.get_allocator().alloc(
                           ObBitVector::memory_size(ctx.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      ObBitVector &mock_skip = *to_bit_vector(skip_mem);
      mock_skip.reset(ctx.max_batch_size_);
      while (OB_SUCC(ret)) {
        int64_t read_rows = 0;
        if (OB_FAIL(ad_result->get_next_unique_hash_table_batch(aggr_info.param_exprs_,
                                                                ctx.max_batch_size_, read_rows))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            SQL_LOG(WARN, "get row from distinct set failed", K(ret));
          }
          break;
        } else if (read_rows <= 0) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "read unexpected zero rows", K(ret));
        } else {
          SQL_LOG(DEBUG, "read rows", K(read_rows), K(ctx.max_batch_size_));
          sql::EvalBound bound(read_rows, true);
          if (OB_FAIL(static_cast<Aggregate *>(agg_)->add_batch_rows(
                agg_ctx, agg_col_id, mock_skip, bound, const_cast<char *>(agg_cell)))) {
            SQL_LOG(WARN, "add batch rows failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && aggr_info.has_order_by_
          && OB_FAIL(static_cast<Aggregate *>(agg_)->eval_group_extra_result(agg_ctx, agg_col_id,
                                                                             group_id))) {
        SQL_LOG(WARN, "eval_inner_agg_failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ad_result->brs_holder_.restore())) {
      SQL_LOG(WARN, "restore datum failed", K(ret));
    }
    return ret;
  }

  template <typename ResultFmt>
  int collect_group_result(RuntimeContext &agg_ctx, const ObExpr &agg_expr, int32_t agg_col_id,
                           const char *agg_cell, const int32_t agg_cell_len)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(static_cast<Aggregate *>(agg_)->template collect_group_result<ResultFmt>(
          agg_ctx, agg_expr, agg_col_id, agg_cell, agg_cell_len))) {
      SQL_LOG(WARN, "collect group result failed", K(ret));
    }
    return ret;
  }

  inline int add_one_row(RuntimeContext &agg_ctx, int64_t batch_idx, int64_t batch_size,
                         const bool is_null, const char *data, const int32_t data_len,
                         int32_t agg_col_idx, char *agg_cell) override
  {
    // FIXME: opt performance
    sql::EvalBound bound(batch_size, batch_idx, batch_idx + 1, true);
    char mock_skip_data[1] = {0};
    ObBitVector &mock_skip = *to_bit_vector(mock_skip_data);
    return static_cast<Aggregate *>(agg_)->add_batch_rows(agg_ctx, agg_col_idx, mock_skip, bound,
                                                          agg_cell);
  }
  void reuse() override
  {
    if (agg_ != NULL) {
      agg_->reuse();
    }
  }

  void destroy() override
  {
    if (agg_ != NULL) {
      agg_->destroy();
      agg_ = nullptr;
    }
  }
  TO_STRING_KV("wrapper_type", "distinct", KP_(agg));

private:
  IAggregate *agg_;
};

template<typename Aggregate>
class GroupStoreWrapper : public BatchAggregateWrapper<GroupStoreWrapper<Aggregate>>
{
  using BaseClass = BatchAggregateWrapper<GroupStoreWrapper<Aggregate>>;
public:
  static const VecValueTypeClass IN_TC = Aggregate::IN_TC;
  static const VecValueTypeClass OUT_TC = Aggregate::OUT_TC;
public:
  GroupStoreWrapper(): agg_(nullptr) {}

  inline void set_inner_aggregate(IAggregate *agg) override {
    agg_ = agg;
  }

  int init(RuntimeContext &agg_ctx, const int64_t agg_col_id, ObIAllocator &allocator) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(agg_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid null aggregate", K(ret));
    } else if (OB_FAIL(agg_->init(agg_ctx, agg_col_id, allocator))) {
      SQL_LOG(WARN, "init aggregate failed", K(ret));
    }
    return ret;
  }

  int add_batch_rows(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                     const sql::ObBitVector &skip, const sql::EvalBound &bound, char *agg_cell,
                     const RowSelector row_sel = RowSelector{}) override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(agg_cell);
    OB_ASSERT(agg_ != NULL);
    ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
    ObIArray<ObExpr *> &param_exprs = aggr_info.param_exprs_;
    OB_ASSERT(0 < param_exprs.count());

    DataStoreVecExtraResult *extra = agg_ctx.get_extra_data_store(agg_col_id, agg_cell);

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(extra)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid null extra", K(ret), K(agg_col_id), KP(extra));
    } else if (!extra->data_store_is_inited()
               && OB_FAIL(extra->init_data_set(aggr_info, agg_ctx.eval_ctx_,
                                               agg_ctx.op_monitor_info_, agg_ctx.io_event_observer_,
                                               agg_ctx.allocator_, agg_ctx.in_window_func_))) {
      SQL_LOG(WARN, "init_distinct_set failed", K(ret));
    } else {
      sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
      if (row_sel.is_empty()) {
        ret = extra->add_batch(param_exprs, ctx, bound, skip, agg_ctx.allocator_);
      } else {
        ret = extra->add_batch(param_exprs, ctx, bound, skip, row_sel.selector(), row_sel.size(),
                               agg_ctx.allocator_);
      }
      if (OB_FAIL(ret)) {
        SQL_LOG(WARN, "add batch rows failed", K(ret));
      }
    }

    return ret;
  }

  int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx, AggrRowPtr group_row,
                         AggrRowPtr rollup_row, int64_t cur_rollup_group_idx,
                         int64_t max_group_cnt = INT64_MIN) override
  {
    /*
      Never used in Group Concat for now. Please test it when you try to use this function.
      Especially keep eyes on the rewind situation.
    */
    int ret = OB_SUCCESS;
    UNUSEDx(cur_rollup_group_idx, max_group_cnt);
    sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    ObIArray<ObExpr *> &param_exprs = agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_;
    char *curr_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, group_row);
    char *rollup_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, rollup_row);
    DataStoreVecExtraResult *ad_result = agg_ctx.get_extra_data_store(agg_col_idx, curr_agg_cell);
    DataStoreVecExtraResult *rollup_result =
      agg_ctx.get_extra_data_store(agg_col_idx, rollup_agg_cell);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    char *skip_mem = nullptr;
    if (OB_ISNULL(ad_result) || !ad_result->is_inited() || OB_ISNULL(rollup_result)
        || !rollup_result->is_inited()) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "distinct set is NULL", K(ret));
    } else if (OB_FAIL(ad_result->data_store_brs_holder_.save(ctx.max_batch_size_))) {
      SQL_LOG(WARN, "backup datum failed", K(ret));
    } else if (OB_ISNULL(skip_mem = (char *)alloc_guard.get_allocator().alloc(
                           ObBitVector::memory_size(ctx.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      int64_t read_rows = 0;
      ObBitVector &mock_skip = *to_bit_vector(skip_mem);
      mock_skip.reset(ctx.max_batch_size_);
      while (OB_SUCC(ret)) {
        if (OB_FAIL(ad_result->get_next_batch(ctx, param_exprs, read_rows))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            SQL_LOG(WARN, "get row from distinct set failed", K(ret));
          }
          break;
        } else if (read_rows <= 0) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "read unexpected zero rows", K(ret));
        } else {
          sql::EvalBound bound(read_rows, true);
          if (OB_FAIL(
                rollup_result->add_batch(param_exprs, ctx, bound, mock_skip, agg_ctx.allocator_))) {
            SQL_LOG(WARN, "add_row failed", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ad_result->data_store_brs_holder_.restore())) {
      SQL_LOG(WARN, "restore datum failed", K(ret));
    }
    return ret;
  }

  int eval_group_extra_result(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                              const int32_t group_id) override
  {
    int ret = OB_SUCCESS;
    OB_ASSERT(agg_col_id < agg_ctx.aggr_infos_.count());
    OB_ASSERT(agg_ctx.aggr_infos_.at(agg_col_id).expr_ != NULL);
    ObExpr *agg_expr = agg_ctx.aggr_infos_.at(agg_col_id).expr_;
    char *agg_cell = nullptr;
    int32_t agg_cell_len = 0;
    agg_ctx.get_agg_payload(agg_col_id, group_id, (const char *&)agg_cell, agg_cell_len);
    OB_ASSERT(agg_ != NULL);
    sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    DataStoreVecExtraResult *data_result = static_cast<DataStoreVecExtraResult *>(
      agg_ctx.get_extra_data_store(agg_col_id, (const char *)agg_cell));
    ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (OB_ISNULL(data_result) || !data_result->data_store_is_inited()) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "Invalid null extra or extra not inited", K(ret));
    } else if (OB_FAIL(data_result->data_store_brs_holder_.save(ctx.max_batch_size_))) {
      SQL_LOG(WARN, "backup datum failed", K(ret));
    } else if (agg_ctx.has_rollup_ && group_id > 0) {
      ret = OB_NOT_IMPLEMENT;
      // Should not be here. Merge rollupllup is not supported. Hash based rollup will not reach
      // here.
    } else if (OB_FAIL(data_result->prepare_for_eval())) {
      SQL_LOG(WARN, "prepare fetch failed", K(ret));
    } else if (agg_ctx.is_in_window_func() && aggr_info.get_expr_type() == T_FUN_GROUP_CONCAT) {
      // Reset the output string length
      *reinterpret_cast<int32_t *>(agg_cell + sizeof(char **) + sizeof(int32_t)) = 0;
      *reinterpret_cast<int64_t *>(agg_cell + sizeof(char **) + 2 * sizeof(int32_t)) = 0;
    }
    char *skip_mem = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(skip_mem = (char *)alloc_guard.get_allocator().alloc(
                           ObBitVector::memory_size(ctx.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      ObBitVector &mock_skip = *to_bit_vector(skip_mem);
      mock_skip.reset(ctx.max_batch_size_);
      while (OB_SUCC(ret)) {
        int64_t read_rows = 0;
        if (OB_FAIL(data_result->get_next_batch(ctx, aggr_info.param_exprs_, read_rows))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            SQL_LOG(WARN, "get row from distinct set failed", K(ret));
          }
          break;
        } else if (read_rows <= 0) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "read unexpected zero rows", K(ret));
        } else {
          sql::EvalBound bound(read_rows, true);
          if (OB_FAIL(static_cast<Aggregate *>(agg_)->add_batch_rows(agg_ctx, agg_col_id, mock_skip,
                                                                     bound, agg_cell))) {
            SQL_LOG(WARN, "add batch rows failed", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(data_result->data_store_brs_holder_.restore())) {
      SQL_LOG(WARN, "restore datum failed", K(ret));
    }
    return ret;
  }

  template <typename ResultFmt>
  int collect_group_result(RuntimeContext &agg_ctx, const ObExpr &agg_expr, int32_t agg_col_id,
                           const char *agg_cell, const int32_t agg_cell_len)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(static_cast<Aggregate *>(agg_)->template collect_group_result<ResultFmt>(
          agg_ctx, agg_expr, agg_col_id, agg_cell, agg_cell_len))) {
      SQL_LOG(WARN, "collect group result failed", K(ret));
    }
    return ret;
  }

  inline int add_one_row(RuntimeContext &agg_ctx, int64_t batch_idx, int64_t batch_size,
                         const bool is_null, const char *data, const int32_t data_len,
                         int32_t agg_col_idx, char *agg_cell) override
  {
    // FIXME: opt performance
    sql::EvalBound bound(batch_size, batch_idx, batch_idx + 1, true);
    char mock_skip_data[1] = {0};
    ObBitVector &mock_skip = *to_bit_vector(mock_skip_data);
    return static_cast<Aggregate *>(agg_)->add_batch_rows(agg_ctx, agg_col_idx, mock_skip, bound,
                                                          agg_cell);
  }
  void reuse() override
  {
    if (agg_ != NULL) {
      agg_->reuse();
    }
  }

  void destroy() override
  {
    if (agg_ != NULL) {
      agg_->destroy();
      agg_ = nullptr;
    }
  }
  TO_STRING_KV("wrapper_type", "distinct", KP_(agg));

private:
  IAggregate *agg_;
};

//helper functions
namespace helper
{
int init_aggregates(RuntimeContext &agg_ctx, ObIAllocator &allocator,
                    ObIArray<IAggregate *> &aggregates);

int init_aggregate(RuntimeContext &agg_ctx, ObIAllocator &allocator, const int64_t col_id,
                   IAggregate *&aggregate);

inline constexpr bool is_var_len_agg_cell(VecValueTypeClass vec_tc)
{
  return vec_tc == VEC_TC_STRING
         || vec_tc == VEC_TC_LOB
         || vec_tc == VEC_TC_ROWID
         || vec_tc == VEC_TC_RAW
         || vec_tc == VEC_TC_JSON
         || vec_tc == VEC_TC_GEO
         || vec_tc == VEC_TC_UDT
         || vec_tc == VEC_TC_COLLECTION
         || vec_tc == VEC_TC_ROARINGBITMAP
         || vec_tc == VEC_TC_EXTEND;
}

template <typename AggType>
int init_agg_func(RuntimeContext &agg_ctx, const int64_t agg_col_id, const bool has_distinct,
                  ObIAllocator &allocator, IAggregate *&agg, const bool need_group_extra = false)
{
  int ret = OB_SUCCESS;
  void *agg_buf = nullptr, *wrapper_buf = nullptr;
  sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
  IAggregate *wrapper = nullptr;
  if (OB_ISNULL(agg_buf = allocator.alloc(sizeof(AggType)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_LOG(WARN, "allocate memory failed", K(ret));
  } else if (FALSE_IT(agg = new (agg_buf) AggType())) {
  }

  if (OB_SUCC(ret) && need_group_extra) {
    if (OB_ISNULL(wrapper_buf = allocator.alloc(sizeof(GroupStoreWrapper<AggType>)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else if (FALSE_IT(wrapper = new (wrapper_buf) GroupStoreWrapper<AggType>())) {
    } else {
      wrapper->set_inner_aggregate(agg);
      agg = wrapper;
    }
    if (has_distinct) {
      wrapper_buf = nullptr;
      wrapper = nullptr;
      if (OB_ISNULL(wrapper_buf =
                      allocator.alloc(sizeof(DistinctWrapper<GroupStoreWrapper<AggType>>)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else if (FALSE_IT(wrapper =
                            new (wrapper_buf) DistinctWrapper<GroupStoreWrapper<AggType>>())) {
      } else {
        wrapper->set_inner_aggregate(agg);
        agg = wrapper;
      }
    }
  } else if (OB_SUCC(ret) && has_distinct) {
    // TODO: min, max, bit_and, bit_or can remove distinct
    if (OB_ISNULL(wrapper_buf = allocator.alloc(sizeof(DistinctWrapper<AggType>)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else if (FALSE_IT(wrapper = new (wrapper_buf) DistinctWrapper<AggType>())) {
    } else {
      wrapper->set_inner_aggregate(agg);
      agg = wrapper;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(agg->init(agg_ctx, agg_col_id, allocator))) {
      SQL_LOG(WARN, "init aggregate failed", K(ret));
    }
  }
  return ret;
}

template <typename AggType>
int init_agg_func(RuntimeContext &agg_ctx, const int64_t agg_col_id, ObIAllocator &allocator,
                  IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  void *agg_buf = nullptr, *wrapper_buf = nullptr;
  sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
  IAggregate *wrapper = nullptr;
  if (OB_ISNULL(agg_buf = allocator.alloc(sizeof(AggType)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_LOG(WARN, "allocate memory failed", K(ret));
  } else if (FALSE_IT(agg = new (agg_buf) AggType())) {
  } else if (OB_FAIL(agg->init(agg_ctx, agg_col_id, allocator))) {
    SQL_LOG(WARN, "init aggregate failed", K(ret));
  }
  return ret;
}

} // end namespace helper
} // end namespace aggregate
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_IAGGREGATE_H_