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
#include "sql/engine/basic/ob_compact_row.h"

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

  inline int add_batch_for_multi_groups(RuntimeContext &agg_ctx, AggrRowPtr *agg_rows,
                                        RowSelector &row_sel, const int64_t batch_size,
                                        const int32_t agg_col_id) override
  {
#define INNER_ADD(vec_tc)                                                                          \
  case (vec_tc): {                                                                                 \
    ret = inner_add_for_multi_groups<ObFixedLengthFormat<RTCType<vec_tc>>>(                        \
      agg_ctx, agg_rows, row_sel, batch_size, agg_col_id, param_expr->get_vector(eval_ctx));       \
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
        ret = inner_add_for_multi_groups<ObUniformFormat<false>>(
          agg_ctx, agg_rows, row_sel, batch_size, agg_col_id, param_expr->get_vector(eval_ctx));
        break;
      }
      case common::VEC_UNIFORM_CONST: {
        ret = inner_add_for_multi_groups<ObUniformFormat<true>>(
          agg_ctx, agg_rows, row_sel, batch_size, agg_col_id, param_expr->get_vector(eval_ctx));
        break;
      }
      case common::VEC_DISCRETE: {
        ret = inner_add_for_multi_groups<ObDiscreteFormat>(
          agg_ctx, agg_rows, row_sel, batch_size, agg_col_id, param_expr->get_vector(eval_ctx));
        break;
      }
      case common::VEC_CONTINUOUS: {
        ret = inner_add_for_multi_groups<ObContinuousFormat>(
          agg_ctx, agg_rows, row_sel, batch_size, agg_col_id, param_expr->get_vector(eval_ctx));
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
                                  const ObBitVector *skip = nullptr) override
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
      if (OB_FAIL(agg_expr.init_vector_for_write(
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

  int collect_batch_group_results(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                                  const int32_t output_start_idx, const int32_t batch_size,
                                  const ObCompactRow **rows, const RowMeta &row_meta) override
  {
    int ret = OB_SUCCESS;
    OB_ASSERT(agg_col_id < agg_ctx.aggr_infos_.count());
    OB_ASSERT(agg_ctx.aggr_infos_.at(agg_col_id).expr_ != NULL);

    ObExpr &agg_expr = *agg_ctx.aggr_infos_.at(agg_col_id).expr_;
    if (OB_LIKELY(batch_size > 0)) {
      if (OB_FAIL(agg_expr.init_vector_for_write(
            agg_ctx.eval_ctx_, agg_expr.get_default_res_format(), batch_size))) {
        SQL_LOG(WARN, "init vector for write failed", K(ret));
      } else {
        VectorFormat res_fmt = agg_expr.get_format(agg_ctx.eval_ctx_);
        VecValueTypeClass res_tc = get_vec_value_tc(
          agg_expr.datum_meta_.type_, agg_expr.datum_meta_.scale_, agg_expr.datum_meta_.precision_);
        switch (res_fmt) {
        case VEC_FIXED: {
          ret = collect_group_results<fixlen_fmt<Derived::OUT_TC>>(
            agg_ctx, agg_col_id, output_start_idx, batch_size, rows, row_meta);
          break;
        }
        case VEC_DISCRETE: {
          ret = collect_group_results<discrete_fmt<Derived::OUT_TC>>(
            agg_ctx, agg_col_id, output_start_idx, batch_size, rows, row_meta);
          break;
        }
        case VEC_UNIFORM_CONST: {
          // must be null type
          ret = collect_group_results<uniform_fmt<VEC_TC_NULL, true>>(
            agg_ctx, agg_col_id, output_start_idx, batch_size, rows, row_meta);
          break;
        }
        case VEC_UNIFORM: {
          // must be null type
          ret = collect_group_results<uniform_fmt<VEC_TC_NULL, false>>(
            agg_ctx, agg_col_id, output_start_idx, batch_size, rows, row_meta);
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
                                 ObIVector *ivec)
  {
    int ret = OB_SUCCESS;
    ColumnFmt *param_vec = static_cast<ColumnFmt *>(ivec);
    bool is_null = false;
    const char *payload = nullptr;
    int32_t len = 0;
    Derived *derived_this = static_cast<Derived *>(this);
    for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
      int64_t batch_idx = row_sel.index(i);
      param_vec->get_payload(batch_idx, is_null, payload, len);
      char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_rows[batch_idx]);
      if (OB_FAIL(derived_this->add_one_row(agg_ctx, batch_idx, batch_size, is_null, payload, len,
                                            agg_col_id, agg_cell))) {
        SQL_LOG(WARN, "inner add one row failed", K(ret));
      }
    }
    return ret;
  }
  template <typename ColumnFmt>
  int add_batch_rows(RuntimeContext &agg_ctx, const sql::ObBitVector &skip,
                     const sql::EvalBound &bound, const ObExpr &param_expr,
                     const int32_t agg_col_id, char *agg_cell, const RowSelector row_sel)
  {
    int ret = OB_SUCCESS;
    ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    ColumnFmt &columns = *static_cast<ColumnFmt *>(param_expr.get_vector(ctx));
    bool all_not_null = !columns.has_null();
    Derived &derived = *static_cast<Derived *>(this);
    void *tmp_res = derived.get_tmp_res(agg_ctx, agg_col_id, agg_cell);
    int64_t calc_info = derived.get_batch_calc_info(agg_ctx, agg_col_id, agg_cell);
    if (OB_LIKELY(!agg_ctx.removal_info_.enable_removal_opt_)) {
      if (OB_LIKELY(row_sel.is_empty() && bound.get_all_rows_active())) {
        if (all_not_null) {
          for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
            if (OB_FAIL(
                  derived.add_row(agg_ctx, columns, i, agg_col_id, agg_cell, tmp_res, calc_info))) {
              SQL_LOG(WARN, "add row failed", K(ret));
            }
          } // end for
          if (agg_cell != nullptr) {
            NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
            not_nulls.set(agg_col_id);
          }
        } else {
          for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
            if (OB_FAIL(derived.add_nullable_row(agg_ctx, columns, i, agg_col_id, agg_cell, tmp_res,
                                                 calc_info))) {
              SQL_LOG(WARN, "add row failed", K(ret));
            }
          } // end for
        }
      } else if (!row_sel.is_empty()) {
        if (all_not_null) {
          for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
            if (OB_FAIL(derived.add_row(agg_ctx, columns, row_sel.index(i), agg_col_id, agg_cell,
                                        tmp_res, calc_info))) {
              SQL_LOG(WARN, "add row failed", K(ret));
            }
          } // end for
          if (agg_cell != nullptr) {
            NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
            not_nulls.set(agg_col_id);
          }
        } else {
          for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
            if (OB_FAIL(derived.add_nullable_row(agg_ctx, columns, row_sel.index(i), agg_col_id,
                                                 agg_cell, tmp_res, calc_info))) {
              SQL_LOG(WARN, "add row failed", K(ret));
            }
          } // end for
        }
      } else {
        if (all_not_null) {
          for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
            if (skip.at(i)) {
            } else if (OB_FAIL(derived.add_row(agg_ctx, columns, i, agg_col_id, agg_cell, tmp_res,
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
            } else if (OB_FAIL(derived.add_nullable_row(agg_ctx, columns, i, agg_col_id, agg_cell,
                                                        tmp_res, calc_info))) {
              SQL_LOG(WARN, "add row failed", K(ret));
            }
          } // end for
        }
      }
    } else {
      if (row_sel.is_empty()) {
        if (bound.get_all_rows_active()) {
          for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
            ret = removal_opt<Derived>::add_or_sub_row(derived, agg_ctx, columns, i, agg_col_id, agg_cell,
                                                       tmp_res, calc_info);
            if (OB_FAIL(ret)) { SQL_LOG(WARN, "add or sub row failed", K(ret)); }
          }
        } else {
          for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
            if (skip.at(i)) {
            } else {
              ret = removal_opt<Derived>::add_or_sub_row(derived, agg_ctx, columns, i, agg_col_id, agg_cell,
                                                         tmp_res, calc_info);
              if (OB_FAIL(ret)) { SQL_LOG(WARN, "add or sub row failed", K(ret)); }
            }
          }
        }
      } else {
        for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
          ret = removal_opt<Derived>::add_or_sub_row(derived, agg_ctx, columns, i, agg_col_id, agg_cell,
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
    ObEvalCtx::BatchInfoScopeGuard guard(agg_ctx.eval_ctx_);

    const char *agg_cell = nullptr;
    int32_t agg_cell_len = 0;
    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      if (skip != nullptr && skip->at(start_output_idx + i)) { continue; }
      guard.set_batch_idx(start_output_idx + i);
      agg_cell = nullptr;
      agg_cell_len = 0;
      agg_ctx.get_agg_payload(agg_col_id, start_gid + i, agg_cell, agg_cell_len);
      if (OB_FAIL(static_cast<Derived *>(this)->template collect_group_result<ResultFmt>(
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
                            const ObCompactRow **rows, const RowMeta &row_meta)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(rows)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid null rows", K(ret));
    } else {
      ObEvalCtx::BatchInfoScopeGuard batch_guard(agg_ctx.eval_ctx_);
      ObExpr *agg_expr = agg_ctx.aggr_infos_.at(agg_col_id).expr_;
      const char *agg_cell = nullptr;
      int32_t agg_cell_len = 0;
      if (OB_FAIL(agg_expr->init_vector_for_write(
            agg_ctx.eval_ctx_, agg_expr->get_default_res_format(), batch_size))) {
        SQL_LOG(WARN, "init vector for write failed", K(ret));
      }
      for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
        batch_guard.set_batch_idx(output_start_idx + i);
        if (OB_ISNULL(rows[i])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "invalid compact row", K(ret), K(i));
        } else {
          const char *agg_row = static_cast<const char *>(rows[i]->get_extra_payload(row_meta));
          agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_row);
          agg_cell_len = agg_ctx.row_meta().get_cell_len(agg_col_id, agg_row);
          SQL_LOG(DEBUG, "collect group results", K(agg_col_id),
                  K(agg_ctx.aggr_infos_.at(agg_col_id).get_expr_type()), KP(agg_cell),
                  K(agg_cell_len), K(agg_cell), K(row_meta), KP(rows[i]));
          if (OB_FAIL(static_cast<Derived *>(this)->template collect_group_result<ResultFmt>(
                agg_ctx, *agg_expr, agg_col_id, agg_cell, agg_cell_len))) {
            SQL_LOG(WARN, "collect group result failed", K(ret));
          }
        }
      }
    }
    return ret;
  }
  template <>
  int collect_group_results<ObVectorBase>(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                                          const int32_t output_start_idx, const int32_t batch_size,
                                          const ObCompactRow **rows, const RowMeta &row_meta)
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

  inline void set_inner_aggregate(IAggregate *agg) override { agg_ = agg; }

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
    int64_t stored_row_cnt = 0;
    sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    ObIArray<ObExpr *> &param_exprs = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_;
    ObAggregateProcessor::ExtraResult* &extra = agg_ctx.get_extra(agg_col_id, agg_cell);
    ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
    if (extra == nullptr) {
      void *tmp_buf = nullptr;
      if (OB_ISNULL(tmp_buf =
                      agg_ctx.allocator_.alloc(sizeof(ObAggregateProcessor::ExtraResult)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else if (OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected null exec session", K(ret));
      } else if (OB_ISNULL(agg_ctx.op_monitor_info_) || OB_ISNULL(agg_ctx.io_event_observer_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid null aggregatec ctx", K(ret), K(agg_ctx.op_monitor_info_),
                K(agg_ctx.io_event_observer_));
      } else {
        extra = new (tmp_buf)
          ObAggregateProcessor::ExtraResult(agg_ctx.allocator_, *agg_ctx.op_monitor_info_);
        const bool need_rewind = false; // TODO: rollup need rewind
        if (OB_FAIL(extra->init_distinct_set(
              ctx.exec_ctx_.get_my_session()->get_effective_tenant_id(), aggr_info, ctx,
              need_rewind, agg_ctx.io_event_observer_))) {
          SQL_LOG(WARN, "init distinct set failed", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(extra) || OB_ISNULL(extra->unique_sort_op_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid null extra", K(ret));
    } else if (!row_sel.is_empty()) {
      if (OB_FAIL(extra->unique_sort_op_->add_batch(param_exprs, skip, bound.batch_size(),
                                                    row_sel.selector(), row_sel.size()))) {
        SQL_LOG(WARN, "add batch failed", K(ret));
      }
    } else if (OB_FAIL(extra->unique_sort_op_->add_batch(param_exprs, skip, bound.batch_size(),
                                                         bound.start(), &stored_row_cnt))) {
      SQL_LOG(WARN, "add batch rows failed", K(ret));
    }
    return ret;
  }
  template <typename ResultFmt>
  int collect_group_result(RuntimeContext &agg_ctx, const ObExpr &agg_expr, int32_t agg_col_id,
                           const char *agg_cell, const int32_t agg_cell_len)
  {
    // TODO: deal with rollup, sort need rewind
    // TODO: advance collect result
    UNUSED(agg_cell_len);
    int ret = OB_SUCCESS;
    const int64_t constexpr max_batch_size = 256;
    char skip_vector[max_batch_size] = {0};
    ObBitVector &mock_skip = *to_bit_vector(skip_vector);
    OB_ASSERT(agg_ != NULL);
    sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    ObAggregateProcessor::ExtraResult *&extra = agg_ctx.get_extra(agg_col_id, agg_cell);
    ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    int32_t tmp_data_len = 0;
    if (OB_ISNULL(extra) || OB_ISNULL(extra->unique_sort_op_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid null extra", K(ret));
    } else if (OB_FAIL(extra->unique_sort_op_->sort())) {
      SQL_LOG(WARN, "sort failed", K(ret));
    } else {
      while(OB_SUCC(ret)) {
        int64_t read_rows = 0;
        if (OB_FAIL(extra->unique_sort_op_->get_next_batch(aggr_info.param_exprs_, max_batch_size,
                                                           read_rows))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            SQL_LOG(WARN, "read batch rows failed", K(ret));
          }
          break;
        } else if (read_rows <= 0) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "read unexpected zero rows", K(ret));
        } else {
          SQL_LOG(DEBUG, "read rows", K(read_rows), K(max_batch_size));
          sql::EvalBound bound(read_rows, true);
          if (OB_FAIL(static_cast<Aggregate *>(agg_)->add_batch_rows(
                agg_ctx, agg_col_id, mock_skip, bound, const_cast<char *>(agg_cell)))) {
            SQL_LOG(WARN, "add batch rows failed", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (FALSE_IT(tmp_data_len = agg_ctx.row_meta().get_cell_len(agg_col_id, agg_cell))) {
      } else if (OB_FAIL(static_cast<Aggregate *>(agg_)->template collect_group_result<ResultFmt>(
                   agg_ctx, agg_expr, agg_col_id, agg_cell, tmp_data_len))) {
        SQL_LOG(WARN, "collect group result failed", K(ret));
      }
    }
    return ret;
  }
  template <typename ColumnFmt>
  inline int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *aggr_cell, void *tmp_res, int64_t &calc_info)
  {
    UNUSEDx(agg_ctx, columns, row_num, agg_col_id, aggr_cell, tmp_res, calc_info);
    SQL_LOG(DEBUG, "add_row do nothing");
    return OB_SUCCESS;
  }
  template <typename ColumnFmt>
  inline int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                              const int32_t agg_col_id, char *agg_cell, void *tmp_res,
                              int64_t &calc_info)
  {
    UNUSEDx(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info);
    SQL_LOG(DEBUG, "add_nullable_row do nothing");
    return OB_SUCCESS;
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
         || vec_tc == VEC_TC_ROARINGBITMAP
         || vec_tc == VEC_TC_EXTEND;
}

template <typename AggType>
int init_agg_func(RuntimeContext &agg_ctx, const int64_t agg_col_id, const bool has_distinct,
                  ObIAllocator &allocator, IAggregate *&agg)
{
  int ret = OB_SUCCESS;
  void *agg_buf = nullptr, *wrapper_buf = nullptr;
  sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
  IAggregate *wrapper = nullptr;
  if (OB_ISNULL(agg_buf = allocator.alloc(sizeof(AggType)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_LOG(WARN, "allocate memory failed", K(ret));
  } else if (FALSE_IT(agg = new (agg_buf) AggType())) {
  } else if (has_distinct) {
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