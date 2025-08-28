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
#ifndef OCEANBASE_SHARE_AGGREGATE_HYBRID_HIST_H
#define OCEANBASE_SHARE_AGGREGATE_HYBRID_HIST_H

#include "share/aggregate/iaggregate.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "share/stat/ob_hybrid_hist_estimator.h"
namespace oceanbase
{
namespace share
{
namespace aggregate
{

class HybridHist final 
  : public BatchAggregateWrapper<HybridHist>
{
public:
  static const constexpr VecValueTypeClass IN_TC = VEC_TC_NULL;
  static const constexpr VecValueTypeClass OUT_TC = VEC_TC_LOB;
  
public:
  HybridHist() {}

  int add_batch_rows(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                     const sql::ObBitVector &skip, const sql::EvalBound &bound, char *agg_cell,
                     const RowSelector row_sel = RowSelector{}) override
  {
    int ret = OB_SUCCESS;
    ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
    ObIArray<ObExpr *> &param_exprs = aggr_info.param_exprs_;
    ObExpr *param_expr = param_exprs.at(0);
    const ObObjMeta &obj_meta = param_expr->obj_meta_;
    if (obj_meta.is_string_type() && !obj_meta.is_lob_storage()) {
      ret = inner_add_batch_rows<true, false>(agg_ctx, skip, bound, *param_expr, 
                                               agg_col_id, agg_cell, row_sel);
    } else if (obj_meta.is_lob_storage()) {
      ret = inner_add_batch_rows<false, true>(agg_ctx, skip, bound, *param_expr, 
                                               agg_col_id, agg_cell, row_sel);
    } else {
      ret = inner_add_batch_rows<false, false>(agg_ctx, skip, bound, *param_expr, 
                                               agg_col_id, agg_cell, row_sel);
    }
    if (OB_FAIL(ret)) {
      SQL_LOG(WARN, "failed to add batch rows", K(ret));
    }
    return ret;
  }

  template<bool is_string, bool is_lob>
  int inner_add_batch_rows(RuntimeContext &agg_ctx, const sql::ObBitVector &skip,
                           const sql::EvalBound &bound, const ObExpr &param_expr,
                           const int32_t agg_col_id, char *agg_cell,
                           const RowSelector row_sel)
  {
    int ret = OB_SUCCESS;
    sql::ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    NullSafeRowCmpFunc cmp_func = param_expr.basic_funcs_->row_null_last_cmp_;
    ObVectorBase &columns = *static_cast<ObVectorBase *>(param_expr.get_vector(ctx));
    HybridHistVecExtraResult *extra = agg_ctx.get_extra_hybrid_hist_store(agg_col_id, agg_cell);
    const ObObjMeta &obj_meta = param_expr.obj_meta_;
    const char *payload = nullptr;
    int32_t len = 0;
    bool all_not_null = !columns.has_null();
    if (row_sel.is_empty()) {
      if (all_not_null && bound.get_all_rows_active()) {
        for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
          columns.get_payload(i, payload, len);
          ret = add_row<is_string, is_lob>(agg_ctx, agg_col_id, agg_cell, 
                                           extra, cmp_func, 
                                           obj_meta, payload, len);
        }
      } else if (all_not_null) {
        for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
          if (skip.at(i)) {
            // do nothing
          } else {
            columns.get_payload(i, payload, len);
            ret = add_row<is_string, is_lob>(agg_ctx, agg_col_id, agg_cell, 
                                             extra, cmp_func, 
                                             obj_meta, payload, len);
          }
        }
      } else {
        for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
          if (skip.at(i)) {
            // do nothing
          } else if (columns.is_null(i)) {
            extra->inc_null_count();
          } else {
            columns.get_payload(i, payload, len);
            ret = add_row<is_string, is_lob>(agg_ctx, agg_col_id, agg_cell, 
                                             extra, cmp_func, 
                                             obj_meta, payload, len);
          }
        }
      }
    } else {
      if (all_not_null) {
        for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
          int row_num = row_sel.index(i);
          columns.get_payload(row_num, payload, len);
          ret = add_row<is_string, is_lob>(agg_ctx, agg_col_id, agg_cell, 
                                           extra, cmp_func, 
                                           obj_meta, payload, len);
        }
      } else {
        for (int i = 0; OB_SUCC(ret) && i < row_sel.size(); i++) {
          int row_num = row_sel.index(i);
          if (columns.is_null(row_num)) {
            extra->inc_null_count();
          } else {
            columns.get_payload(row_num, payload, len);
            ret = add_row<is_string, is_lob>(agg_ctx, agg_col_id, agg_cell, 
                                             extra, cmp_func, 
                                             obj_meta, payload, len);
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      SQL_LOG(WARN, "failed to add batch rows", K(ret));
    } else if (OB_FAIL(extra->flush_batch_rows(true))) {
      SQL_LOG(WARN, "failed to flush batch rows", K(ret));
    } else if (is_lob) {
      extra->get_lob_prefix_allocator().reuse();
    }
    return ret;
  }


  template<bool is_string, bool is_lob>
  OB_INLINE int add_row(RuntimeContext &agg_ctx,
                        const int32_t agg_col_id, 
                        const char *agg_cell,
                        HybridHistVecExtraResult *extra, 
                        NullSafeRowCmpFunc cmp_func,
                        const ObObjMeta &obj_meta,
                        const char* payload,
                        int len)
  {
    int ret = OB_SUCCESS;
    int cmp_ret = -1;
    if (is_string) {
      if (len > OPT_STATS_MAX_VALUE_CHAR_LEN) {
        ObCollationType cs_type = obj_meta.get_collation_type();
        int64_t mb_len = ObCharset::strlen_char(cs_type, payload, len);
        if (mb_len <= OPT_STATS_MAX_VALUE_CHAR_LEN) {
          // do nothing
        } else {
          len = ObCharset::charpos(cs_type, payload, len, OPT_STATS_MAX_VALUE_CHAR_LEN);
        }
      }
    } else if (is_lob) {
      ObDatum tmp_datum;
      ObDatum new_prev_datum;
      tmp_datum.ptr_ = payload;
      tmp_datum.pack_ = len;
      if (OB_FAIL(ObHybridHistograms::build_prefix_str_datum_for_lob(extra->get_lob_prefix_allocator(),
                                                                     obj_meta,
                                                                     tmp_datum,
                                                                     new_prev_datum))) {
       SQL_LOG(WARN, "failed to build prefix str datum for lob", K(ret));
      } else {
        payload = new_prev_datum.ptr_;
        len = new_prev_datum.pack_;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(extra->get_num_distinct() == 0)) {
      DataStoreVecExtraResult *data_store = agg_ctx.get_extra_data_store(agg_col_id, 
                                                                         agg_cell);
      extra->add_payload(payload, len);
      extra->calc_total_count(data_store->get_sort_count());                                                               
    } else if (OB_FAIL(cmp_func(obj_meta,
                                obj_meta,
                                payload, len, false,
                                extra->get_prev_payload(), 
                                extra->get_prev_payload_len(),
                                false, cmp_ret))) {
      SQL_LOG(WARN, "failed to cmp null first row", K(ret));
    } else if (cmp_ret == 0) {
      extra->inc_repeat_count();
    } else if (OB_FAIL(extra->add_one_batch_item(payload, len))) {
      SQL_LOG(WARN, "failed to add one batch item", K(ret));
    }
    return ret;
  }

  template <typename ColumnFmt>
  int collect_group_result(RuntimeContext &agg_ctx, const sql::ObExpr &agg_expr,
                           const int32_t agg_col_id, const char *agg_cell,
                           const int32_t agg_cell_len)
  {
    int ret = OB_SUCCESS;
    ObHybridHistograms hybrid_hist;
    ColumnFmt *res_vec = static_cast<ColumnFmt *>(agg_expr.get_vector(agg_ctx.eval_ctx_));
    HybridHistVecExtraResult *extra = agg_ctx.get_extra_hybrid_hist_store(agg_col_id, agg_cell);
    int64_t output_idx = agg_ctx.eval_ctx_.get_batch_idx();
    const ObObjMeta &obj_meta = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.at(0)->obj_meta_;
    if (OB_ISNULL(extra)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN , "invalid null top fre hist extra", K(ret));
    } else if (OB_FAIL(extra->prepare_for_eval())) {
      SQL_LOG(WARN, "failed to eval", K(ret));
    } else if (OB_UNLIKELY(extra->get_num_distinct() <= 0)) {
      res_vec->set_null(output_idx);
    } else if (OB_FAIL(extra->compute_hybrid_hist_result(agg_ctx.eval_ctx_.max_batch_size_,
                                                         obj_meta,
                                                         agg_ctx.allocator_,
                                                         hybrid_hist))) {
      SQL_LOG(WARN, "failed to compute hybrid hist result", K(ret));
    } else {
      char *buf = NULL;
      int64_t buf_size = hybrid_hist.get_serialize_size();
      int64_t buf_pos = 0;
      bool has_lob_header = agg_expr.obj_meta_.has_lob_header();
      ObExprStrResAlloc expr_res_alloc(agg_expr, agg_ctx.eval_ctx_);
      ObTextStringResult new_tmp_lob(ObLongTextType, has_lob_header, &expr_res_alloc);
      if (OB_FAIL(new_tmp_lob.init(buf_size))) {
        SQL_LOG(WARN, "init tmp lob failed", K(ret), K(buf_size));
      } else if (OB_FAIL(new_tmp_lob.get_reserved_buffer(buf, buf_size))) {
        SQL_LOG(WARN, "tmp lob append failed", K(ret), K(new_tmp_lob));
      } else if (OB_FAIL(hybrid_hist.serialize(buf, buf_size, buf_pos))) {
        SQL_LOG(WARN, "fail serialize init task arg", KP(buf), K(buf_size), K(buf_pos), K(ret));
      } else if (OB_FAIL(new_tmp_lob.lseek(buf_pos, 0))) {
        SQL_LOG(WARN, "temp lob lseek failed", K(ret), K(new_tmp_lob), K(buf_pos));
      } else {
        ObString lob_loc_str;
        new_tmp_lob.get_result_buffer(lob_loc_str);
        res_vec->set_payload_shallow(output_idx, lob_loc_str.ptr(), lob_loc_str.length());
      }
    }
    return ret;
  }
  
  inline int add_one_row(RuntimeContext &agg_ctx, int64_t row_num, int64_t batch_size,
                         const bool is_null, const char *data, const int32_t data_len,
                         int32_t agg_col_idx, char *agg_cell) override
  {
    int ret = OB_NOT_SUPPORTED;
    SQL_LOG(WARN, "hybrid hist not support add one row", K(ret));
    return ret;
  }

  virtual int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx,
                                 AggrRowPtr group_row, AggrRowPtr rollup_row,
                                 int64_t cur_rollup_group_idx,
                                 int64_t max_group_cnt = INT64_MIN) override
  {
    int ret = OB_NOT_SUPPORTED;
    SQL_LOG(WARN, "hybrid hist not support in group by rollup", K(ret));
    return ret;
  }

  int eval_group_extra_result(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                              const int32_t cur_group_id) override
  {
    int ret = OB_SUCCESS;
    return ret;
  }

  TO_STRING_KV("aggregate", "hybrid_hist");
};

}
}
}

#endif