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
#ifndef OCEANBASE_SHARE_AGGREGATE_TOP_FRE_HIST_H
#define OCEANBASE_SHARE_AGGREGATE_TOP_FRE_HIST_H

#include "share/aggregate/iaggregate.h"
#include "share/stat/ob_topk_hist_estimator.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "share/stat/ob_hybrid_hist_estimator.h"
namespace oceanbase
{
namespace share
{
namespace aggregate
{

template <VecValueTypeClass in_tc, bool is_merge>
class TopFreHist final 
  : public BatchAggregateWrapper<TopFreHist<in_tc, is_merge>>
{
  static const constexpr int64_t TOPK_HISTOGRAMS_BUF_LENGTH = sizeof(ObTopKFrequencyHistograms);
public:
  static const constexpr VecValueTypeClass IN_TC = in_tc;
  static const constexpr VecValueTypeClass OUT_TC = VEC_TC_LOB;
public:
  TopFreHist() {}

  template <typename ColumnFmt>
  OB_INLINE int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    const char *payload = nullptr;
    int32_t len = 0;
    HashFuncTypeForTc hash_func = reinterpret_cast<HashFuncTypeForTc>(calc_info);
    TopFreHistVecExtraResult *extra = reinterpret_cast<TopFreHistVecExtraResult *>(tmp_res);
    columns.get_payload(row_num, payload, len);
    if (OB_FAIL(add_top_k_frequency_item(agg_ctx, extra, hash_func, agg_col_id, payload, len))) {
      SQL_LOG(WARN, "failed to add top k frequency item", K(ret));
    }
    return ret;
  }

  OB_INLINE int add_top_k_frequency_item(RuntimeContext &agg_ctx, 
                                         TopFreHistVecExtraResult *extra,
                                         HashFuncTypeForTc hash_func,
                                         const int32_t agg_col_id,
                                         const char *payload,
                                         int32_t len)
  {
    int ret = OB_SUCCESS;
    ObDatum tmp_datum;
    uint64_t hash_val = 0;
    const ObExpr *param_expr = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.at(0);
    if (is_merge) {
      ObObj obj;
      tmp_datum.ptr_ = payload;
      tmp_datum.pack_ = len;
      if (OB_FAIL(tmp_datum.to_obj(obj, param_expr->obj_meta_))) {
        SQL_LOG(WARN, "failed to obj", K(ret));
      } else if (OB_FAIL(extra->get_topk_hist().merge_distribute_top_k_fre_items(obj))) {
        SQL_LOG(WARN, "failed to process row", K(ret));
      }
    } else if (extra->get_topk_hist().is_by_pass()) {
      extra->inc_disuse_cnt();
    } else if (is_lob_vec_tc()) {
      const ObObjMeta obj_meta = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.at(0)->obj_meta_;
      ObDatum new_prev_datum;
      tmp_datum.ptr_ = payload;
      tmp_datum.pack_ = len;
      if (OB_FAIL(ObHybridHistograms::build_prefix_str_datum_for_lob(extra->get_lob_prefix_allocator(),
                                                                     obj_meta,
                                                                     tmp_datum,
                                                                     new_prev_datum))) {
       SQL_LOG(WARN, "failed to build prefix str datum for lob", K(ret));
      } else if (OB_FAIL(hash_func(param_expr->obj_meta_, new_prev_datum.ptr_, new_prev_datum.pack_, hash_val, hash_val))) {
        SQL_LOG(WARN, "hash func failed", K(ret));
      } else if (OB_FAIL(extra->add_one_batch_item(new_prev_datum.ptr_, new_prev_datum.pack_, hash_val))) {
        SQL_LOG(WARN, "failed to add one batch item", K(ret));
      } 
    } else if (in_tc == VEC_TC_STRING) {
      int64_t truncated_str_len = 0;
      truncated_str_len = len;
      if (len > OPT_STATS_MAX_VALUE_CHAR_LEN) {
        ObCollationType cs_type = param_expr->obj_meta_.get_collation_type();
        int64_t mb_len = ObCharset::strlen_char(cs_type, payload, len);
        if (mb_len <= OPT_STATS_MAX_VALUE_CHAR_LEN) {//keep origin str
          truncated_str_len = len;
        } else {
          truncated_str_len = ObCharset::charpos(cs_type, payload, len, OPT_STATS_MAX_VALUE_CHAR_LEN);
        }
      }
      if (OB_FAIL(hash_func(param_expr->obj_meta_, payload, truncated_str_len, hash_val, hash_val))) {
        SQL_LOG(WARN, "hash func failed", K(ret));
      } else if (OB_FAIL(extra->add_one_batch_item(payload, truncated_str_len, hash_val))) {
        SQL_LOG(WARN, "failed to add one batch item", K(ret));
      }
    } else {
      if (OB_FAIL(hash_func(param_expr->obj_meta_, payload, len, hash_val, hash_val))) {
        SQL_LOG(WARN, "hash func failed", K(ret));
      } else if (OB_FAIL(extra->add_one_batch_item(payload, len, hash_val))) {
        SQL_LOG(WARN, "failed to add one batch item", K(ret));
      }
    }
    return ret;
  }

  template <typename ColumnFmt>
  OB_INLINE int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                       const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(columns.is_null(row_num))) {
      SQL_LOG(DEBUG, "add null row", K(ret), K(row_num));
    } else if (OB_FAIL(
                 add_row(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info))) {
      SQL_LOG(WARN, "add row failed", K(ret));
    } else {
      NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
      not_nulls.set(agg_col_id);
    }
    return ret;
  }

  int collect_tmp_result(RuntimeContext &agg_ctx, const int32_t agg_col_id, char *agg_cell)
  {
    int ret = OB_SUCCESS;
    TopFreHistVecExtraResult *extra = agg_ctx.get_extra_top_fre_hist_store(agg_col_id, agg_cell);
    if (is_merge || extra->get_topk_hist().is_by_pass()) {
      // do nothing
    } else if (OB_FAIL(extra->flush_batch_rows())) {
      SQL_LOG(WARN, "failed to flush batch rows", K(ret));
    } else if (is_lob_vec_tc()) {
      extra->get_lob_prefix_allocator().reuse();
    }
    return ret;
  }

  template <typename ColumnFmt>
  int collect_group_result(RuntimeContext &agg_ctx, const sql::ObExpr &agg_expr,
                           const int32_t agg_col_id, const char *agg_cell,
                           const int32_t agg_cell_len)
  {
    int ret = OB_SUCCESS;
    const NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    int64_t output_idx = agg_ctx.eval_ctx_.get_batch_idx();
    ColumnFmt *res_vec = static_cast<ColumnFmt *>(agg_expr.get_vector(agg_ctx.eval_ctx_));
    TopFreHistVecExtraResult *extra = agg_ctx.get_extra_top_fre_hist_store(agg_col_id, agg_cell);
    if (OB_ISNULL(extra)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN , "invalid null top fre hist extra", K(ret));
    } else if (OB_LIKELY(not_nulls.at(agg_col_id))) {
      ObTopKFrequencyHistograms &top_k_fre_hist = extra->get_topk_hist();
      if (top_k_fre_hist.has_bucket() ||
          (top_k_fre_hist.is_by_pass() && !top_k_fre_hist.is_need_merge_topk_hist())) {
        bool has_lob_header = agg_expr.obj_meta_.has_lob_header();
        const ObObjMeta &obj_meta = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.at(0)->obj_meta_;
        if (OB_FAIL(top_k_fre_hist.create_topk_fre_items(&obj_meta))) {
          SQL_LOG(WARN, "failed to adjust frequency sort", K(ret));
        } else {
          char *buf = NULL;
          int64_t buf_size = top_k_fre_hist.get_serialize_size();
          int64_t buf_pos = 0;
          ObExprStrResAlloc expr_res_alloc(agg_expr, agg_ctx.eval_ctx_);
          ObTextStringResult new_tmp_lob(ObLongTextType, has_lob_header, &expr_res_alloc);
          if (OB_FAIL(new_tmp_lob.init(buf_size))) {
            SQL_LOG(WARN, "init tmp lob failed", K(ret), K(buf_size));
          } else if (OB_FAIL(new_tmp_lob.get_reserved_buffer(buf, buf_size))) {
            SQL_LOG(WARN, "tmp lob append failed", K(ret), K(new_tmp_lob));
          } else if (OB_FAIL(top_k_fre_hist.serialize(buf, buf_size, buf_pos))) {
            SQL_LOG(WARN, "fail serialize init task arg", KP(buf), K(buf_size), K(buf_pos), K(ret));
          } else if (OB_FAIL(new_tmp_lob.lseek(buf_pos, 0))) {
            SQL_LOG(WARN, "temp lob lseek failed", K(ret), K(new_tmp_lob), K(buf_pos));
          } else {
            ObString lob_loc_str;
            new_tmp_lob.get_result_buffer(lob_loc_str);
            res_vec->set_payload_shallow(output_idx, lob_loc_str.ptr(), lob_loc_str.length());
          }
        }
      } else {
        res_vec->set_null(output_idx);
      }
    } else {
      res_vec->set_null(output_idx);
    }
    return ret;
  }
  
  inline int add_one_row(RuntimeContext &agg_ctx, int64_t row_num, int64_t batch_size,
                         const bool is_null, const char *data, const int32_t data_len,
                         int32_t agg_col_idx, char *agg_cell) override
  {
    int ret = OB_SUCCESS;
    if (OB_LIKELY(agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_.count() == 1)) {
      const ObExpr *param_expr = agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_.at(0);
      HashFuncTypeForTc hash_func = VecTCHashCalc<in_tc, ObMurmurHash, true>::hash;
      TopFreHistVecExtraResult *extra = agg_ctx.get_extra_top_fre_hist_store(agg_col_idx, agg_cell);
      if (OB_LIKELY(!is_null)) {
        if (OB_FAIL(add_top_k_frequency_item(agg_ctx, 
                                             extra, 
                                             hash_func, 
                                             agg_col_idx, 
                                             data, 
                                             data_len))) {
          SQL_LOG(WARN, "failed to add top k frequency item", K(ret));
        } else if (!is_merge && 
                   OB_FAIL(extra->flush_batch_rows())) {
          SQL_LOG(WARN, "failed to flush batch rows", K(ret));
        } else if (!is_merge && is_lob_vec_tc()) {
          extra->get_lob_prefix_allocator().reuse();
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      SQL_LOG(WARN, "unsupported top fre hist", K(ret));
    }
    return ret;
  }

  inline bool is_lob_vec_tc() 
  {
    return in_tc == VEC_TC_LOB ||
           in_tc == VEC_TC_JSON ||
           in_tc == VEC_TC_GEO ||
           in_tc == VEC_TC_ROARINGBITMAP ||
           in_tc == VEC_TC_COLLECTION;
  }
  virtual int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx,
                                 AggrRowPtr group_row, AggrRowPtr rollup_row,
                                 int64_t cur_rollup_group_idx,
                                 int64_t max_group_cnt = INT64_MIN) override
  {
    int ret = OB_NOT_SUPPORTED;
    SQL_LOG(WARN, "topk fre hist not support in group by rollup", K(ret));
    return ret;
  }

  inline void *get_tmp_res(RuntimeContext &agg_ctx, int32_t agg_col_id, char *agg_cell) override
  {
    TopFreHistVecExtraResult *extra = agg_ctx.get_extra_top_fre_hist_store(agg_col_id, agg_cell);
    OB_ASSERT(extra != NULL);
    return extra;
  }

  inline int64_t get_batch_calc_info(RuntimeContext &agg_ctx, 
                                     int32_t agg_col_id,
                                     char *agg_cell) override
  {
    HashFuncTypeForTc hash_func = VecTCHashCalc<in_tc, ObMurmurHash, true>::hash;
    return reinterpret_cast<int64_t>(hash_func);
  }

  int eval_group_extra_result(RuntimeContext &agg_ctx, const int32_t agg_col_id,
                              const int32_t cur_group_id) override
  {
    int ret = OB_SUCCESS;
    return ret;
  }

  TO_STRING_KV("aggregate", "top_fre_hist", K(in_tc));
};

}
}
}

#endif