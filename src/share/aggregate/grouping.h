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

#ifndef OCEANBASE_SHARE_GROUPING_AGGREGATE_H_
#define OCEANBASE_SHARE_GROUPING_AGGREGATE_H_

#include "share/aggregate/iaggregate.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{

bool is_grouping(const ObAggrInfo &aggr_info, const int64_t val);
int get_grouping_id(const ObAggrInfo &aggr_info, const int64_t val, number::ObCompactNumber *grouping_id);
int get_grouping_id(const ObAggrInfo &aggr_info, const int64_t val, int64_t *grouping_id);

template<ObExprOperatorType agg_func, VecValueTypeClass out_tc>
class GroupingAggregate final: public BatchAggregateWrapper<GroupingAggregate<agg_func, out_tc>>
{
public:
  static const constexpr VecValueTypeClass IN_TC = VEC_TC_NULL;
  static const constexpr VecValueTypeClass OUT_TC = out_tc;

public:
  GroupingAggregate() {}

  virtual int add_batch_rows(RuntimeContext &agg_ctx, int32_t agg_col_idx,
                             const sql::ObBitVector &skip, const sql::EvalBound &bound,
                             char *agg_cell, const RowSelector row_sel = RowSelector{}) override
  {
    int ret = OB_SUCCESS;
    ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_idx);
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell);
#ifndef NDEBUG
    helper::print_input_rows(row_sel, skip, bound, agg_ctx.aggr_infos_.at(agg_col_idx), false,
                             agg_ctx.eval_ctx_, this, agg_col_idx);
#endif
    if (OB_ISNULL(aggr_info.hash_rollup_info_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected null rollup grouping id", K(ret));
    } else if (not_nulls.at(agg_col_idx)) {// already calculated, do nothing
    } else if (row_sel.is_empty()) {
      ObIVector *grouping_vec = aggr_info.hash_rollup_info_->rollup_grouping_id_->get_vector(agg_ctx.eval_ctx_);
      bool found = false;
      for (int i = bound.start(); !found && i < bound.end(); i++) {
        if (skip.at(i)) {
        } else if (agg_func == T_FUN_GROUPING) {
          *reinterpret_cast<int64_t *>(agg_cell) = is_grouping(aggr_info, grouping_vec->get_int(i));
          found = true;
        } else if (agg_func == T_FUN_GROUPING_ID) {
          if (OB_UNLIKELY(out_tc != VEC_TC_NUMBER && out_tc != VEC_TC_INTEGER)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_LOG(WARN, "invalid out type class", K(ret));
          } else if (out_tc == VEC_TC_NUMBER && OB_FAIL(get_grouping_id(aggr_info, grouping_vec->get_int(i),
                                                                        reinterpret_cast<number::ObCompactNumber *>(agg_cell)))) {
            SQL_LOG(WARN, "get grouping id failed", K(ret));
          } else if (out_tc == VEC_TC_INTEGER && OB_FAIL(get_grouping_id(aggr_info, grouping_vec->get_int(i), 
                                                                         reinterpret_cast<int64_t *>(agg_cell)))) {
            SQL_LOG(WARN, "get grouping id failed", K(ret));
          } else {
            found = true;
          }
        } else {
          ob_assert(false);
        }
      }
      if (OB_SUCC(ret) && found) {
        not_nulls.set(agg_col_idx);
      }
    } else {
      ObIVector *grouping_vec = aggr_info.hash_rollup_info_->rollup_grouping_id_->get_vector(agg_ctx.eval_ctx_);
      bool found = false;
      for (int i = 0; OB_SUCC(ret) && !found && i < row_sel.size(); i++) {
        if (agg_func == T_FUN_GROUPING) {
          *reinterpret_cast<int64_t *>(agg_cell) = is_grouping(aggr_info, grouping_vec->get_int(row_sel.index(i)));
          found = true;
        } else if (agg_func == T_FUN_GROUPING_ID) {
          if (OB_UNLIKELY(out_tc != VEC_TC_NUMBER && out_tc != VEC_TC_INTEGER)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_LOG(WARN, "invalid out type class", K(ret));
          } else if (out_tc == VEC_TC_NUMBER && OB_FAIL(get_grouping_id(aggr_info, grouping_vec->get_int(row_sel.index(i)),
                                                                        reinterpret_cast<number::ObCompactNumber *>(agg_cell)))) {
            SQL_LOG(WARN, "get grouping id failed", K(ret));
          } else if (out_tc == VEC_TC_INTEGER && OB_FAIL(get_grouping_id(aggr_info, grouping_vec->get_int(row_sel.index(i)), 
                                                                         reinterpret_cast<int64_t *>(agg_cell)))) {
            SQL_LOG(WARN, "get grouping id failed", K(ret));
          } else {
            found = true;
          }
        } else {
          ob_assert(false);
        }
      }
      if (OB_SUCC(ret) && found) {
        not_nulls.set(agg_col_idx);
      }
    }
    return ret;
  }

  virtual int add_one_row(RuntimeContext &agg_ctx, const int64_t batch_idx,
                          const int64_t batch_size, const bool is_null, const char *data,
                          const int32_t data_len, int32_t agg_col_idx, char *agg_cell) override
  {
    int ret = OB_SUCCESS;
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell);
    ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_idx);
    if (OB_ISNULL(aggr_info.hash_rollup_info_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid null rollup grouping id", K(ret));
    } else if (not_nulls.at(agg_col_idx)) { // already calculated, do nothing
    } else {
      ObIVector *grouping_vec = aggr_info.hash_rollup_info_->rollup_grouping_id_->get_vector(agg_ctx.eval_ctx_);
      if (agg_func == T_FUN_GROUPING) {
        *reinterpret_cast<int64_t *>(agg_cell) = is_grouping(aggr_info, grouping_vec->get_int(batch_idx));
        not_nulls.set(agg_col_idx);
      } else if (agg_func == T_FUN_GROUPING_ID) {
        if (OB_UNLIKELY(out_tc != VEC_TC_NUMBER && out_tc != VEC_TC_INTEGER)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "invalid out type class", K(ret));
        } else if (out_tc == VEC_TC_NUMBER && OB_FAIL(get_grouping_id(aggr_info, grouping_vec->get_int(batch_idx),
                                                                      reinterpret_cast<number::ObCompactNumber *>(agg_cell)))) {
          SQL_LOG(WARN, "get grouping id failed", K(ret));
        } else if (out_tc == VEC_TC_INTEGER && OB_FAIL(get_grouping_id(aggr_info, grouping_vec->get_int(batch_idx), 
                                                                       reinterpret_cast<int64_t *>(agg_cell)))) {
          SQL_LOG(WARN, "get grouping id failed", K(ret));
        } else {
          not_nulls.set(agg_col_idx);
        }
      } else {
        ob_assert(false);
      }
    }
    return ret;
  }

  virtual int add_batch_for_multi_groups(RuntimeContext &agg_ctx, AggrRowPtr *agg_rows,
                                         RowSelector &row_sel, const int64_t batch_size,
                                         const int32_t agg_col_id)
  {
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
      int row_idx = row_sel.index(i);
      char *agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_id, agg_rows[row_idx]);
      // grouping/grouping_id do not use payload/len to caluclate results
      // just pass nullptr
      if (OB_FAIL(add_one_row(agg_ctx, row_idx, batch_size, true, nullptr, 0, agg_col_id, agg_cell))) {
        SQL_LOG(WARN, "add_one_row failed", K(ret));
      }
    }
    return ret;
  }

  template<typename ColumnFormat>
  int collect_group_result(RuntimeContext &agg_ctx, const ObExpr &agg_expr,
                           const int32_t agg_col_id, const char *data, const int32_t data_len)
  {
    int ret = OB_SUCCESS;
    ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_id);
    const NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, data);
    ColumnFormat *out_vec = static_cast<ColumnFormat *>(agg_expr.get_vector(agg_ctx.eval_ctx_));
    int64_t output_idx = agg_ctx.eval_ctx_.get_batch_idx();
    if (OB_UNLIKELY(!not_nulls.at(agg_col_id))) {
      out_vec->set_null(output_idx);
    } else if (agg_func == T_FUN_GROUPING) {
      if (out_tc == VEC_TC_INTEGER) {
        out_vec->set_int(output_idx, *reinterpret_cast<const int64_t *>(data));
      } else if(out_tc == VEC_TC_NUMBER) {
        sql::ObNumStackOnceAlloc tmp_alloc;
        number::ObNumber tmp_nmb;
        if (OB_FAIL(tmp_nmb.from(*reinterpret_cast<const int64_t *>(data), tmp_alloc))) {
          SQL_LOG(WARN, "number::from failed", K(ret));
        } else {
          out_vec->set_number(output_idx, tmp_nmb);
        }
      } else {
        ob_assert(false);
      }
    } else if (agg_func == T_FUN_GROUPING_ID) {
      if (out_tc == VEC_TC_NUMBER) {
        const number::ObCompactNumber *res_cnum = reinterpret_cast<const number::ObCompactNumber *>(data);
        number::ObNumber res_num(*res_cnum);
        out_vec->set_number(output_idx, res_num);
      } else if (out_tc == VEC_TC_INTEGER) {
        out_vec->set_int(output_idx, *reinterpret_cast<const int64_t *>(data));
      } else {
        ob_assert(false);
      }
    } else {
      ob_assert(false);
    }
    return ret;
  }

  virtual int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx,
                                 AggrRowPtr group_row, AggrRowPtr rollup_row,
                                 int64_t cur_rollup_group_idx,
                                 int64_t max_group_cnt = INT64_MIN) override
  {
    return OB_NOT_IMPLEMENT;
  }
  template <typename ColumnFmt>
  int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
              const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    return OB_NOT_IMPLEMENT;
  }
  template <typename ColumnFmt>
  int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                       const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    return OB_NOT_IMPLEMENT;
  }

  TO_STRING_KV("aggregate", "grouping", K(agg_func));
};
} // end aggregate
} // end share
} // end oceanbase
#endif // OCEANBASE_SHARE_GROUPING_AGGREGATE_H_