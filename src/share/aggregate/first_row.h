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

#ifndef OCEANBASE_SHARE_AGGREGATE_FIRST_ROW_H_
#define OCEANBASE_SHARE_AGGREGATE_FIRST_ROW_H_

#include "iaggregate.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{

/*
 first row are stored as <char *, len> in aggregate row
    count       sum       first_row_data
 ------------------------------------------------------
 | int64_t | ob_number | <data_ptr, data_len> |  ...
 ------------------------------------------------------
                            |
                            |                    |----------------|
                            |------------------->| copied payload |
                                                 |----------------|
*/
template<VecValueTypeClass vec_tc>
class FirstRowAggregate final: public BatchAggregateWrapper<FirstRowAggregate<vec_tc>>
{
public:
  static const VecValueTypeClass IN_TC = vec_tc;
  static const VecValueTypeClass OUT_TC = vec_tc;
public:
  FirstRowAggregate() {}
  inline int add_batch_rows(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                            const sql::ObBitVector &skip, const sql::EvalBound &bound,
                            char *agg_cell, const RowSelector row_sel = RowSelector{}) override
  {
    SQL_LOG(DEBUG, "implicit first row", K(agg_col_id),
            K(*agg_ctx.aggr_infos_.at(agg_col_id).expr_), K(bound));
    int ret = OB_SUCCESS;
    ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
    const char *payload = nullptr;
    bool null_cell = false;
    int32_t data_len = 0;
#ifndef NDEBUG
    helper::print_input_rows(row_sel, skip, bound, agg_ctx.aggr_infos_.at(agg_col_id), true,
                             agg_ctx.eval_ctx_, this, agg_col_id);
#endif
    if (OB_LIKELY(not_nulls.at(agg_col_id))) {
      // already copied
    } else if (OB_ISNULL(aggr_info.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected null expr", K(ret));
    } else if (row_sel.is_empty()) {
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
        if (!bound.get_all_rows_active() && skip.at(i)) { // do nothing
        } else {
          ObIVector *data_vec = aggr_info.expr_->get_vector(ctx);
          if (data_vec->is_null(i)) {
            null_cell = true;
          } else {
            data_vec->get_payload(i, payload, data_len);
          }
          break;
        }
      }
    } else {
      for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
        ObIVector *data_vec = aggr_info.expr_->get_vector(ctx);
        if (data_vec->is_null(row_sel.index(i))) {
          null_cell = true;
        } else {
          aggr_info.expr_->get_vector(ctx)->get_payload(row_sel.index(i), payload, data_len);
        }
        break;
      }
    }
    if (OB_SUCC(ret) && (payload != nullptr || null_cell)) {
      if (null_cell) {
        agg_ctx.set_agg_cell(nullptr, INT32_MAX, agg_col_id, agg_cell);
      } else if (data_len > 0) {
        void *tmp_buf = agg_ctx.allocator_.alloc(data_len);
        if (OB_ISNULL(tmp_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_LOG(WARN, "allocate memory failed", K(ret));
        } else {
          MEMCPY(tmp_buf, payload, data_len);
          // store data ptr and len
          agg_ctx.set_agg_cell((char *)tmp_buf, data_len, agg_col_id, agg_cell);
        }
      } else {
        agg_ctx.set_agg_cell(nullptr, data_len, agg_col_id, agg_cell);
      }
      not_nulls.set(agg_col_id);
    }
    return ret;
  }

  inline int add_one_row(RuntimeContext &agg_ctx, int64_t batch_idx, int64_t batch_size,
                         const bool is_null, const char *data, const int32_t data_len,
                         int32_t agg_col_idx, char *agg_cell) override
  {
    int ret = OB_SUCCESS;
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell);
    if (OB_LIKELY(not_nulls.at(agg_col_idx))) {
      // already copied
    } else if (!is_null) {
      if (data_len > 0) {
        if (OB_ISNULL(data)) {
          ret = OB_INVALID_ARGUMENT;
          SQL_LOG(WARN, "invalid null payload", K(ret));
        } else {
          void *tmp_buf = agg_ctx.allocator_.alloc(data_len);
          if (OB_ISNULL(tmp_buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SQL_LOG(WARN, "allocate memory failed", K(ret));
          } else {
            MEMCPY(tmp_buf, data, data_len);
            // store data ptr and len
            agg_ctx.set_agg_cell((char *)tmp_buf, data_len, agg_col_idx, agg_cell);
          }
        }
      } else {
        agg_ctx.set_agg_cell(nullptr, data_len, agg_col_idx, agg_cell);
      }
      not_nulls.set(agg_col_idx);
    } else {
      agg_ctx.set_agg_cell(nullptr, INT32_MAX, agg_col_idx, agg_cell);
      not_nulls.set(agg_col_idx);
    }
    return ret;
  }

  template <typename ColumnFmt>
  inline int collect_group_result(RuntimeContext &agg_ctx, const sql::ObExpr &agg_expr,
                                  const int32_t agg_col_id, const char *agg_cell,
                                  const int32_t agg_cell_len)
  {
    int ret = OB_SUCCESS;
    ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    int64_t output_idx = ctx.get_batch_idx();
    const NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    ColumnFmt *res_vec = static_cast<ColumnFmt *>(agg_expr.get_vector(ctx));
    if (OB_LIKELY(not_nulls.at(agg_col_id) && agg_cell_len != INT32_MAX)) {
      const char *payload = (const char *)(*reinterpret_cast<const int64_t *>(agg_cell));
      char *res_buf = nullptr;
      if (is_discrete_vec(vec_tc)) {
        // implicit aggr expr may be shared between operators and its
        // data is shallow copied for variable-length types while do backup/restore operations.
        // Hence child op's data is unexpected modified if deep copy happened here.
        // see details in `bug/55372943`
        //
        // note that variable-length types include ObNumberType.
        res_vec->set_payload_shallow(output_idx, payload, agg_cell_len);
      } else {
        CellWriter<AggCalcType<vec_tc>>::set(payload, agg_cell_len, res_vec, output_idx, res_buf);
      }
    } else {
      res_vec->set_null(output_idx);
    }
    return ret;
  }

  template<typename ColumnFmt>
  inline int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    UNUSEDx(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info);
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

  TO_STRING_KV("aggregate", "first_row");
};

inline int init_first_row_aggregate(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                                    ObIAllocator &allocator, IAggregate *&agg)
{
#define INIT_FIRST_ROW_AGG_CASE(vec_tc)                                                            \
  case (vec_tc): {                                                                                 \
    if (OB_ISNULL(buf = allocator.alloc(sizeof(FirstRowAggregate<vec_tc>)))) {                     \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                             \
      SQL_LOG(WARN, "allocate memory failed", K(ret));                                             \
    } else {                                                                                       \
      agg = new (buf) FirstRowAggregate<vec_tc>();                                                 \
    }                                                                                              \
  } break
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObExpr *agg_expr = agg_ctx.aggr_infos_.at(agg_col_id).expr_;
  if (OB_ISNULL(agg_expr)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "invalid null aggregate expr", K(ret));
  } else {
    VecValueTypeClass res_vec = get_vec_value_tc(
      agg_expr->datum_meta_.type_, agg_expr->datum_meta_.scale_, agg_expr->datum_meta_.precision_);
    switch (res_vec) {
      LST_DO_CODE(INIT_FIRST_ROW_AGG_CASE, AGG_VEC_TC_LIST);
      default: {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "invalid result tc", K(ret), K(res_vec));
      }
    }
  }
  return ret;
}
} // end aggregate
} // end share
} // end oceanbase
#endif // OCEANBASE_SHARE_AGGREGATE_FIRST_ROW_H_