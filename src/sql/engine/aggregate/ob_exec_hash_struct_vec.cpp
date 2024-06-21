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

#include "ob_exec_hash_struct_vec.h"
#include "common/row/ob_row_store.h"
#include "share/aggregate/processor.h"
#include "sql/engine/aggregate/ob_hash_groupby_vec_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ShortStringAggregator::fallback_calc_hash_value_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                                                          const ObBatchRows &child_brs,
                                                          ObEvalCtx &eval_ctx,
                                                          uint64_t *hash_values)
{
  int ret = OB_SUCCESS;
  uint64_t *new_hash_values = hash_values;
  uint64_t seed = ObHashGroupByVecOp::HASH_SEED;
  //firstly calc hash value for batch
  for (int64_t i = 0; OB_SUCC(ret) && i < gby_exprs.count(); ++i) {
    ObExpr *expr = gby_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid expr", K(ret), K(i));
    } else {
      const bool is_batch_seed = (i > 0);
      ObIVector *col_vec = expr->get_vector(eval_ctx);
      if (OB_FAIL(col_vec->murmur_hash_v3(*expr, new_hash_values, *child_brs.skip_,
                                          EvalBound(child_brs.size_, child_brs.all_rows_active_),
                                          is_batch_seed ? new_hash_values : &seed, is_batch_seed))) {
        LOG_WARN("failed to calc hash value", K(ret));
      }
    }
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::set_unique_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                                                           const ObBatchRows &child_brs,
                                                           const uint64_t *hash_values,
                                                           char **batch_new_rows)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && auto_extend_ && OB_UNLIKELY((size_ + child_brs.size_)
                                                      * SIZE_BUCKET_SCALE >= get_bucket_num())) {
    int64_t pre_bkt_num = get_bucket_num();
    if (OB_FAIL(extend())) {
      SQL_ENG_LOG(WARN, "extend failed", K(ret));
    } else if (get_bucket_num() <= pre_bkt_num) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to extend table", K(ret), K(pre_bkt_num), K(get_bucket_num()));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_brs.size_; ++i) {
    if (nullptr == batch_new_rows[i]) {
      continue;
    }
    ObCompactRow &srow = const_cast<ObCompactRow &> (share::aggregate::Processor::
                        get_groupby_stored_row(group_store_.get_row_meta(), batch_new_rows[i]));
    GroupRowBucket *bucket = const_cast<GroupRowBucket *> (&locate_empty_bucket(*buckets_, hash_values[i]));
    bucket->set_hash(hash_values[i]);
    bucket->set_valid();
    bucket->set_bkt_seq(size_);
    bucket->set_item(static_cast<ObGroupRowItemVec &> (srow));
    size_ += 1;
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::append_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                                                       const ObBatchRows &child_brs,
                                                       const bool *is_dumped,
                                                       const uint64_t *hash_values,
                                                       const common::ObIArray<int64_t> &lengths,
                                                       char **batch_new_rows,
                                                       int64_t &agg_group_cnt,
                                                       bool need_reinit_vectors)
{
  int ret = OB_SUCCESS;
  if (!is_inited_vec_ || need_reinit_vectors) {
    for (int64_t i = 0; i < gby_exprs.count(); ++i) {
      if (nullptr == gby_exprs.at(i)) {
        vector_ptrs_.at(i) = nullptr;
      } else {
        vector_ptrs_.at(i) = gby_exprs.at(i)->get_vector(*eval_ctx_);
      }
    }
    is_inited_vec_ = true;
  }
  char *get_row = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(group_store_.add_batch(vector_ptrs_,
                     &new_row_selector_.at(0),
                     new_row_selector_cnt_,
                     srows_,
                     &lengths))) {
    LOG_WARN("failed to add batch rows", K(ret));
  } else {
    agg_group_cnt += new_row_selector_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_row_selector_cnt_; ++i) {
      GroupRowBucket *curr_bkt = locate_buckets_[new_row_selector_.at(i)];
      ObCompactRow *curr_row = srows_[i];
      if (OB_ISNULL(curr_bkt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get locate bucket", K(ret), K(new_row_selector_.at(i)));
      } else {
        curr_bkt->set_hash(hash_values[new_row_selector_.at(i)]);
        curr_bkt->set_valid();
        curr_bkt->set_bkt_seq(size_ + i);
        curr_bkt->set_item(static_cast<ObGroupRowItemVec &> (*curr_row));
        get_row = curr_bkt->get_item().get_aggr_row(group_store_.get_row_meta());
        CK (OB_NOT_NULL(get_row));
      }
      LOG_DEBUG("append new row", "new row",
           CompactRow2STR(group_store_.get_row_meta(), *curr_row, &gby_exprs));
      batch_new_rows[new_row_selector_.at(i)] = get_row;
    }
    size_ += new_row_selector_cnt_;
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::process_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                                                        const ObBatchRows &child_brs,
                                                        const bool *is_dumped,
                                                        const uint64_t *hash_values,
                                                        const common::ObIArray<int64_t> &lengths,
                                                        const bool can_append_batch,
                                                        const ObGbyBloomFilterVec *bloom_filter,
                                                        char **batch_old_rows,
                                                        char **batch_new_rows,
                                                        int64_t &agg_row_cnt,
                                                        int64_t &agg_group_cnt,
                                                        BatchAggrRowsTable *batch_aggr_rows,
                                                        bool need_reinit_vectors)
{
  int ret = OB_SUCCESS;
  if (sstr_aggr_.is_valid()
      && OB_FAIL(sstr_aggr_.check_batch_length(gby_exprs, child_brs, is_dumped,
                                               const_cast<uint64_t *> (hash_values), *eval_ctx_))) {
    LOG_WARN("failed to check batch length", K(ret));
  } else if (!sstr_aggr_.is_valid()) {
    new_row_selector_cnt_ = 0;
    // extend bucket to hold whole batch
    while (OB_SUCC(ret) && auto_extend_ && OB_UNLIKELY((size_ + child_brs.size_)
                                                        * SIZE_BUCKET_SCALE >= get_bucket_num())) {
      int64_t pre_bkt_num = get_bucket_num();
      if (OB_FAIL(extend())) {
        SQL_ENG_LOG(WARN, "extend failed", K(ret));
      } else if (get_bucket_num() <= pre_bkt_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to extend table", K(ret), K(pre_bkt_num), K(get_bucket_num()));
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(locate_buckets_)) {
      if (OB_ISNULL(locate_buckets_ = static_cast<GroupRowBucket **> (allocator_.alloc(sizeof(GroupRowBucket *) * max_batch_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc bucket ptrs", K(ret), K(max_batch_size_));
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(srows_)) {
      if (OB_ISNULL(srows_ = static_cast<ObCompactRow **> (allocator_.alloc(sizeof(ObCompactRow *) * max_batch_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc bucket ptrs", K(ret), K(max_batch_size_));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t start_idx = 0;
      int64_t processed_idx = 0;
      if (OB_FAIL(inner_process_batch(gby_exprs, child_brs, is_dumped,
                                      hash_values, lengths, can_append_batch,
                                      bloom_filter, batch_old_rows, batch_new_rows,
                                      agg_row_cnt, agg_group_cnt, batch_aggr_rows,
                                      need_reinit_vectors, true, start_idx, processed_idx))) {
        LOG_WARN("failed to process batch", K(ret));
      } else if (processed_idx < child_brs.size_
                 && OB_FAIL(inner_process_batch(gby_exprs, child_brs, is_dumped,
                                                hash_values, lengths, can_append_batch,
                                                bloom_filter, batch_old_rows, batch_new_rows,
                                                agg_row_cnt, agg_group_cnt, batch_aggr_rows,
                                                need_reinit_vectors, false, processed_idx, processed_idx))) {
        LOG_WARN("failed to process batch fallback", K(ret));
      }
    }
  } else {
    bool has_new_row = false;
    if (OB_FAIL(sstr_aggr_.process_batch(gby_exprs, child_brs, item_alloc_,
                                   *eval_ctx_, batch_old_rows, batch_new_rows,
                                   agg_row_cnt, agg_group_cnt, batch_aggr_rows,
                                   has_new_row))) {
      LOG_WARN("failed to process batch", K(ret));
    } else if (has_new_row) {
      if (OB_FAIL(ShortStringAggregator::fallback_calc_hash_value_batch(gby_exprs, child_brs, *eval_ctx_,
                                                 const_cast<uint64_t *> (hash_values)))) {
        LOG_WARN("failed to calc hash values", K(ret));
      } else if (OB_FAIL(set_unique_batch(gby_exprs, child_brs, hash_values, batch_new_rows))) {
        LOG_WARN("failed to set unique batch", K(ret));
      }
    }
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::inner_process_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                                                              const ObBatchRows &child_brs,
                                                              const bool *is_dumped,
                                                              const uint64_t *hash_values,
                                                              const common::ObIArray<int64_t> &lengths,
                                                              const bool can_append_batch,
                                                              const ObGbyBloomFilterVec *bloom_filter,
                                                              char **batch_old_rows,
                                                              char **batch_new_rows,
                                                              int64_t &agg_row_cnt,
                                                              int64_t &agg_group_cnt,
                                                              BatchAggrRowsTable *batch_aggr_rows,
                                                              bool need_reinit_vectors,
                                                              const bool probe_by_col,
                                                              const int64_t start_idx,
                                                              int64_t &processed_idx)
{
  int ret = OB_SUCCESS;
  int64_t curr_idx = start_idx;
  bool need_fallback = false;
  while (OB_SUCC(ret) && !need_fallback && curr_idx < child_brs.size_) {
    bool batch_duplicate = false;
    new_row_selector_cnt_ = 0;
    old_row_selector_cnt_ = 0;
    for (; OB_SUCC(ret) && curr_idx < child_brs.size_; ++curr_idx) {
      if (child_brs.skip_->at(curr_idx)
          || is_dumped[curr_idx]
          || (nullptr != bloom_filter
              && !bloom_filter->exist(ObGroupRowBucketBase::HASH_VAL_MASK & hash_values[curr_idx]))) {
        continue;
      }
      int64_t curr_pos = -1;
      bool find_bkt = false;
      while (OB_SUCC(ret) && !find_bkt) {
        locate_buckets_[curr_idx] = const_cast<GroupRowBucket *> (&locate_next_bucket(*buckets_, hash_values[curr_idx], curr_pos));
        if (locate_buckets_[curr_idx]->is_valid()) {
          if (probe_by_col) {
            old_row_selector_.at(old_row_selector_cnt_++) = curr_idx;
            __builtin_prefetch(&locate_buckets_[curr_idx]->get_item(),
                          0, 2);
            find_bkt = true;
          } else {
            bool result = true;
            ObGroupRowItemVec *it = &locate_buckets_[curr_idx]->get_item();
            for (int64_t i = 0; OB_SUCC(ret) && result && i < gby_exprs.count(); ++i) {
              bool null_equal = (nullptr == gby_exprs.at(i));
              if (!null_equal) {
                ObIVector *r_vec = gby_exprs.at(i)->get_vector(*eval_ctx_);
                const bool l_isnull = it->is_null(i);
                const bool r_isnull = r_vec->is_null(curr_idx);
                if (l_isnull != r_isnull) {
                  result = false;
                } else if (l_isnull && r_isnull) {
                  result = true;
                } else {
                  const int64_t l_len = it->get_length(group_store_.get_row_meta(), i);
                  const int64_t r_len = r_vec->get_length(curr_idx);
                  if (l_len == r_len
                      && 0 == memcmp(it->get_cell_payload(group_store_.get_row_meta(), i),
                                  r_vec->get_payload(curr_idx),
                                  r_len)) {
                    result = true;
                  } else {
                    int cmp_res = 0;
                    if (OB_FAIL(r_vec->null_last_cmp(*gby_exprs.at(i), curr_idx, false,
                                                    it->get_cell_payload(group_store_.get_row_meta(), i),
                                                    l_len, cmp_res))) {
                      LOG_WARN("failed to cmp left and right", K(ret));
                    } else {
                      result = (0 == cmp_res);
                    }
                  }
                }
              }
            }
            if (OB_SUCC(ret) && result) {
              ++probe_cnt_;
              ++agg_row_cnt;
              batch_old_rows[curr_idx] = it->get_aggr_row(group_store_.get_row_meta());
              if (batch_aggr_rows && batch_aggr_rows->is_valid()) {
                if (size_ > BatchAggrRowsTable::MAX_REORDER_GROUPS) {
                  batch_aggr_rows->set_invalid();
                } else {
                  int64_t ser_num = locate_buckets_[curr_idx]->get_bkt_seq();
                  batch_aggr_rows->aggr_rows_[ser_num] = batch_old_rows[curr_idx];
                  batch_aggr_rows->selectors_[ser_num][batch_aggr_rows->selectors_item_cnt_[ser_num]++] = curr_idx;
                }
              }
              find_bkt = true;
            }
          }
        } else if (locate_buckets_[curr_idx]->is_occupyed()) {
          batch_duplicate = true;
          find_bkt = true;
        } else if (can_append_batch) {
          //occupy empty bucket
          locate_buckets_[curr_idx]->set_occupyed();
          new_row_selector_.at(new_row_selector_cnt_++) = curr_idx;
          ++probe_cnt_;
          ++agg_row_cnt;
          find_bkt = true;
        } else {
          find_bkt = true;
        }
      }
      if (batch_duplicate) {
        break;
      }
    }
    if (OB_SUCC(ret)
        && probe_by_col
        && old_row_selector_cnt_ > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && !need_fallback && i < gby_exprs.count(); ++i) {
        if (nullptr == gby_exprs.at(i)) {
          //3 stage null equal
          continue;
        }
        col_has_null_.at(i) |= gby_exprs.at(i)->get_vector(*eval_ctx_)->has_null();
        if (OB_FAIL(col_has_null_.at(i) ?
                    inner_process_column(gby_exprs, group_store_.get_row_meta(), i, need_fallback)
                    : inner_process_column_not_null(gby_exprs, group_store_.get_row_meta(), i, need_fallback))) {
          LOG_WARN("failed to process column", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        if (!need_fallback) {
          probe_cnt_ += old_row_selector_cnt_;
          agg_row_cnt += old_row_selector_cnt_;
          if (batch_aggr_rows && batch_aggr_rows->is_valid() && size_ > BatchAggrRowsTable::MAX_REORDER_GROUPS) {
            batch_aggr_rows->set_invalid();
          }
          for (int64_t i = 0; i < old_row_selector_cnt_; ++i) {
            const int64_t idx = old_row_selector_.at(i);
            batch_old_rows[idx] = (static_cast<GroupRowBucket *> (locate_buckets_[idx]))->get_item().get_aggr_row(group_store_.get_row_meta());
            if (batch_aggr_rows && batch_aggr_rows->is_valid()) {
              int64_t ser_num = locate_buckets_[idx]->get_bkt_seq();
              batch_aggr_rows->aggr_rows_[ser_num] = batch_old_rows[idx];
              batch_aggr_rows->selectors_[ser_num][batch_aggr_rows->selectors_item_cnt_[ser_num]++] = idx;
            }
          }
        } else {
          //reset occupyed bkt and stat
          for (int64_t i = 0; i < new_row_selector_cnt_; ++i) {
            locate_buckets_[new_row_selector_.at(i)]->set_empty();
          }
          probe_cnt_ -= new_row_selector_cnt_;
          agg_row_cnt -= new_row_selector_cnt_;
          continue;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (can_append_batch
          && new_row_selector_cnt_ > 0
          && OB_FAIL(append_batch(gby_exprs, child_brs, is_dumped, hash_values,
                              lengths, batch_new_rows, agg_group_cnt,
                              need_reinit_vectors))) {
        LOG_WARN("failed to append batch", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < gby_exprs.count(); ++i) {
          if (nullptr == gby_exprs.at(i)) {
            //3 stage null equal
            continue;
          }
          col_has_null_.at(i) |= gby_exprs.at(i)->get_vector(*eval_ctx_)->has_null();
        }
        new_row_selector_cnt_ = 0;
      }
    }
    processed_idx = curr_idx;
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::inner_process_column(const common::ObIArray<ObExpr *> &gby_exprs,
                                                               const RowMeta &row_meta,
                                                               const int64_t col_idx,
                                                               bool &need_fallback)
{
  int ret = OB_SUCCESS;
  switch (gby_exprs.at(col_idx)->get_format(*eval_ctx_)) {
    case VEC_FIXED : {
      ObFixedLengthBase *r_vec = static_cast<ObFixedLengthBase *> (gby_exprs.at(col_idx)->get_vector(*eval_ctx_));
      int64_t r_len = r_vec->get_length();
      if (row_meta.fixed_expr_reordered()) {
        const int64_t offset = row_meta.get_fixed_cell_offset(col_idx);
        if (r_len == 8) {
          for (int64_t i = 0; !need_fallback && i < old_row_selector_cnt_; ++i) {
            const int64_t curr_idx = old_row_selector_.at(i);
            ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
            const bool l_isnull = it->is_null(col_idx);
            const bool r_isnull = r_vec->get_nulls()->at(curr_idx);
            if (l_isnull != r_isnull) {
              need_fallback = true;
            } else if (l_isnull && r_isnull) {
            } else {
              need_fallback = (*(reinterpret_cast<int64_t *> (r_vec->get_data() + r_len * curr_idx))
                                  != *(reinterpret_cast<const int64_t *> (it->get_fixed_cell_payload(offset))));
            }
          }
        } else {
          for (int64_t i = 0; !need_fallback && i < old_row_selector_cnt_; ++i) {
            const int64_t curr_idx = old_row_selector_.at(i);
            ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
            const bool l_isnull = it->is_null(col_idx);
            const bool r_isnull = r_vec->get_nulls()->at(curr_idx);
            if (l_isnull != r_isnull) {
              need_fallback = true;
            } else if (l_isnull && r_isnull) {
            } else {
              need_fallback = (0 != memcmp(it->get_fixed_cell_payload(offset),
                            r_vec->get_data() + r_len * curr_idx,
                            r_len));
            }
          }
        }
      } else {
        for (int64_t i = 0; !need_fallback && i < old_row_selector_cnt_; ++i) {
          const int64_t curr_idx = old_row_selector_.at(i);
          ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
          const bool l_isnull = it->is_null(col_idx);
          const bool r_isnull = r_vec->get_nulls()->at(curr_idx);
           if (l_isnull != r_isnull) {
            need_fallback = true;
          } else if (l_isnull && r_isnull) {
          } else {
            need_fallback = (0 != memcmp(it->get_cell_payload(row_meta, col_idx),
                            r_vec->get_data() + r_len * curr_idx,
                            r_len));
          }
        }
      }
      break;
    }
    case VEC_DISCRETE : {
      ObDiscreteBase *r_vec = static_cast<ObDiscreteBase *> (gby_exprs.at(col_idx)->get_vector(*eval_ctx_));
      for (int64_t i = 0; OB_SUCC(ret) && !need_fallback && i < old_row_selector_cnt_; ++i) {
        const int64_t curr_idx = old_row_selector_.at(i);
        ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
        const bool l_isnull = it->is_null(col_idx);
        const bool r_isnull = r_vec->get_nulls()->at(curr_idx);
        if (l_isnull != r_isnull) {
          need_fallback = true;
        } else if (l_isnull && r_isnull) {
        } else {
          const int64_t r_len = r_vec->get_lens()[curr_idx];
          const int64_t l_len = it->get_length(row_meta, col_idx);
          if (r_len == l_len
              && 0 == memcmp(it->get_cell_payload(row_meta, col_idx),
                          r_vec->get_ptrs()[curr_idx],
                          r_len)) {
          } else {
            int cmp_res = 0;
            if (OB_FAIL(r_vec->null_last_cmp(*gby_exprs.at(col_idx), curr_idx, false,
                                            it->get_cell_payload(row_meta, col_idx),
                                            l_len, cmp_res))) {
              LOG_WARN("failed to cmp left and right", K(ret));
            } else {
              need_fallback = static_cast<bool> (cmp_res);
            }
          }
        }
      }
      break;
    }
    case VEC_CONTINUOUS :
    case VEC_UNIFORM :
    case VEC_UNIFORM_CONST : {
      ObIVector *r_vec = gby_exprs.at(col_idx)->get_vector(*eval_ctx_);
      for (int64_t i = 0; OB_SUCC(ret) && !need_fallback && i < old_row_selector_cnt_; ++i) {
        const int64_t curr_idx = old_row_selector_.at(i);
        ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
        const bool l_isnull = it->is_null(col_idx);
        const bool r_isnull = r_vec->is_null(curr_idx);
        if (l_isnull != r_isnull) {
          need_fallback = true;
        } else if (l_isnull && r_isnull) {
        } else {
          const int64_t l_len = it->get_length(row_meta, col_idx);
          const int64_t r_len = r_vec->get_length(curr_idx);
          if (l_len == r_len
              && 0 == memcmp(it->get_cell_payload(row_meta, col_idx),
                          r_vec->get_payload(curr_idx),
                          r_len)) {
          } else {
            int cmp_res = 0;
            if (OB_FAIL(r_vec->null_last_cmp(*gby_exprs.at(col_idx), curr_idx, false,
                                            it->get_cell_payload(row_meta, col_idx),
                                            l_len, cmp_res))) {
              LOG_WARN("failed to cmp left and right", K(ret));
            } else {
              need_fallback = static_cast<bool> (cmp_res);
            }
          }
        }
      }
      break;
    }
    default :
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid data format", K(ret), K(gby_exprs.at(col_idx)->get_format(*eval_ctx_)));
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::inner_process_column_not_null(const common::ObIArray<ObExpr *> &gby_exprs,
                                                                        const RowMeta &row_meta,
                                                                        const int64_t col_idx,
                                                                        bool &need_fallback)
{
  int ret = OB_SUCCESS;
  switch (gby_exprs.at(col_idx)->get_format(*eval_ctx_)) {
    case VEC_FIXED : {
      ObFixedLengthBase *r_vec = static_cast<ObFixedLengthBase *> (gby_exprs.at(col_idx)->get_vector(*eval_ctx_));
      int64_t r_len = r_vec->get_length();
      if (row_meta.fixed_expr_reordered()) {
        const int64_t offset = row_meta.get_fixed_cell_offset(col_idx);
        if (r_len == 8) {
          for (int64_t i = 0; !need_fallback && i < old_row_selector_cnt_; ++i) {
            const int64_t curr_idx = old_row_selector_.at(i);
            ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
            need_fallback = (*(reinterpret_cast<int64_t *> (r_vec->get_data() + r_len * curr_idx))
                                != *(reinterpret_cast<const int64_t *> (it->get_fixed_cell_payload(offset))));
          }
        } else {
          for (int64_t i = 0; !need_fallback && i < old_row_selector_cnt_; ++i) {
            const int64_t curr_idx = old_row_selector_.at(i);
            ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
            need_fallback = (0 != memcmp(it->get_fixed_cell_payload(offset),
                          r_vec->get_data() + r_len * curr_idx,
                          r_len));
          }
        }
      } else {
        for (int64_t i = 0; !need_fallback && i < old_row_selector_cnt_; ++i) {
          const int64_t curr_idx = old_row_selector_.at(i);
          ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
          need_fallback = (0 != memcmp(it->get_cell_payload(row_meta, col_idx),
                          r_vec->get_data() + r_len * curr_idx,
                          r_len));
        }
      }
      break;
    }
    case VEC_DISCRETE : {
      ObDiscreteBase *r_vec = static_cast<ObDiscreteBase *> (gby_exprs.at(col_idx)->get_vector(*eval_ctx_));
      for (int64_t i = 0; OB_SUCC(ret) && !need_fallback && i < old_row_selector_cnt_; ++i) {
        const int64_t curr_idx = old_row_selector_.at(i);
        ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
        const int64_t r_len = r_vec->get_lens()[curr_idx];
        const int64_t l_len = it->get_length(row_meta, col_idx);
        if (r_len == l_len
            && 0 == memcmp(it->get_cell_payload(row_meta, col_idx),
                        r_vec->get_ptrs()[curr_idx],
                        r_len)) {
        } else {
          int cmp_res = 0;
          if (OB_FAIL(r_vec->null_last_cmp(*gby_exprs.at(col_idx), curr_idx, false,
                                          it->get_cell_payload(row_meta, col_idx),
                                          l_len, cmp_res))) {
            LOG_WARN("failed to cmp left and right", K(ret));
          } else {
            need_fallback = static_cast<bool> (cmp_res);
          }
        }
      }
      break;
    }
    case VEC_CONTINUOUS :
    case VEC_UNIFORM :
    case VEC_UNIFORM_CONST : {
      ObIVector *r_vec = gby_exprs.at(col_idx)->get_vector(*eval_ctx_);
      for (int64_t i = 0; OB_SUCC(ret) && !need_fallback && i < old_row_selector_cnt_; ++i) {
        const int64_t curr_idx = old_row_selector_.at(i);
        ObCompactRow *it = static_cast<ObCompactRow *> (&locate_buckets_[curr_idx]->get_item());
        const int64_t l_len = it->get_length(row_meta, col_idx);
        const int64_t r_len = r_vec->get_length(curr_idx);
        if (l_len == r_len
            && 0 == memcmp(it->get_cell_payload(row_meta, col_idx),
                        r_vec->get_payload(curr_idx),
                        r_len)) {
        } else {
          int cmp_res = 0;
          if (OB_FAIL(r_vec->null_last_cmp(*gby_exprs.at(col_idx), curr_idx, false,
                                          it->get_cell_payload(row_meta, col_idx),
                                          l_len, cmp_res))) {
            LOG_WARN("failed to cmp left and right", K(ret));
          } else {
            need_fallback = static_cast<bool> (cmp_res);
          }
        }
      }
      break;
    }
    default :
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid data format", K(ret), K(gby_exprs.at(col_idx)->get_format(*eval_ctx_)));
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::init(ObIAllocator *allocator,
                                               lib::ObMemAttr &mem_attr,
                                               const ObIArray<ObExpr *> &gby_exprs,
                                               const int64_t hash_expr_cnt,
                                               ObEvalCtx *eval_ctx,
                                               int64_t max_batch_size,
                                               bool nullable,
                                               bool all_int64,
                                               int64_t op_id,
                                               bool use_sstr_aggr,
                                               int64_t aggr_row_size,
                                               int64_t initial_size,
                                               bool auto_extend)
{
  int ret = OB_SUCCESS;
  if (initial_size < 2) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(initial_size));
  } else {
    mem_attr_ = mem_attr;
    auto_extend_ = auto_extend;
    hash_expr_cnt_ = hash_expr_cnt;
    allocator_.set_allocator(allocator);
    allocator_.set_label(mem_attr.label_);
    max_batch_size_ = max_batch_size;
    void *buckets_buf = NULL;
    op_id_ = op_id;
    vector_ptrs_.set_allocator(allocator);
    new_row_selector_.set_allocator(allocator);
    old_row_selector_.set_allocator(allocator);
    col_has_null_.set_allocator(allocator);
    change_valid_idx_.set_allocator(allocator);
    if (use_sstr_aggr && OB_FAIL(sstr_aggr_.init(allocator_, *eval_ctx, gby_exprs, aggr_row_size))) {
      LOG_WARN("failed to init short string aggr", K(ret));
    } else if (OB_FAIL(vector_ptrs_.prepare_allocate(gby_exprs.count()))) {
      SQL_ENG_LOG(WARN, "failed to alloc ptrs", K(ret));
    } else if (OB_FAIL(new_row_selector_.prepare_allocate(max_batch_size))) {
      SQL_ENG_LOG(WARN, "failed to alloc array", K(ret));
    } else if (OB_FAIL(old_row_selector_.prepare_allocate(max_batch_size))) {
      SQL_ENG_LOG(WARN, "failed to alloc array", K(ret));
    } else if (OB_FAIL(col_has_null_.prepare_allocate(gby_exprs.count()))) {
      SQL_ENG_LOG(WARN, "failed to alloc array", K(ret));
    } else if (OB_ISNULL(buckets_buf = allocator_.alloc(sizeof(BucketArray), mem_attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
    } else {
      MEMSET(&col_has_null_.at(0), 0, col_has_null_.count());
      buckets_ = new(buckets_buf)BucketArray(allocator_);
      buckets_->set_tenant_id(tenant_id_);
      initial_bucket_num_ = common::next_pow2(initial_size * SIZE_BUCKET_SCALE);
      SQL_ENG_LOG(DEBUG, "debug bucket num", K(ret), K(buckets_->count()), K(initial_bucket_num_));
      size_ = 0;
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(extend())) {
      SQL_ENG_LOG(WARN, "extend failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_vec_ = false;
    gby_exprs_ = &gby_exprs;
    eval_ctx_ = eval_ctx;
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::set_distinct_batch(const RowMeta &row_meta,
                                                             const int64_t batch_size,
                                                             const ObBitVector *child_skip,
                                                             ObBitVector &my_skip,
                                                             uint64_t *hash_values,
                                                             StoreRowFunc sf)
{
  int ret = OB_SUCCESS;
  new_row_selector_cnt_ = 0;
  while (OB_SUCC(ret) && auto_extend_ && OB_UNLIKELY((size_ + batch_size)
                                                      * SIZE_BUCKET_SCALE >= get_bucket_num())) {
    int64_t pre_bkt_num = get_bucket_num();
    if (OB_FAIL(extend())) {
      SQL_ENG_LOG(WARN, "extend failed", K(ret));
    } else if (get_bucket_num() <= pre_bkt_num) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to extend table", K(ret), K(pre_bkt_num), K(get_bucket_num()));
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(locate_buckets_)) {
    if (OB_ISNULL(locate_buckets_ = static_cast<GroupRowBucket **> (allocator_.alloc(sizeof(GroupRowBucket *) * max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc bucket ptrs", K(ret), K(max_batch_size_));
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(srows_)) {
    if (OB_ISNULL(srows_ = static_cast<ObCompactRow **> (allocator_.alloc(sizeof(ObCompactRow *) * max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc bucket ptrs", K(ret), K(max_batch_size_));
    }
  }
  if (OB_SUCC(ret) && !is_inited_vec_) {
    for (int64_t i = 0; i < gby_exprs_->count(); ++i) {
      vector_ptrs_.at(i) = gby_exprs_->at(i)->get_vector(*eval_ctx_);
    }
    is_inited_vec_ = true;
  }

  if (OB_SUCC(ret)) {
    const int64_t start_idx = 0;
    int64_t processed_idx = 0;
    if (OB_FAIL(inner_process_batch(row_meta, batch_size, child_skip,
                                    my_skip, hash_values, sf,
                                    true, start_idx, processed_idx))) {
      LOG_WARN("failed to process batch", K(ret));
    } else if (processed_idx < batch_size
                && OB_FAIL(inner_process_batch(row_meta, batch_size, child_skip,
                                               my_skip, hash_values, sf,
                                               false, processed_idx, processed_idx))) {
      LOG_WARN("failed to process batch fallback", K(ret));
    }
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::inner_process_batch(const RowMeta &row_meta,
                                                              const int64_t batch_size,
                                                              const ObBitVector *child_skip,
                                                              ObBitVector &my_skip,
                                                              uint64_t *hash_values,
                                                              StoreRowFunc sf,
                                                              const bool probe_by_col,
                                                              const int64_t start_idx,
                                                              int64_t &processed_idx)
{
  int ret = OB_SUCCESS;
  int64_t curr_idx = start_idx;
  bool need_fallback = false;
  while (OB_SUCC(ret) && !need_fallback && curr_idx < batch_size) {
    bool batch_duplicate = false;
    new_row_selector_cnt_ = 0;
    old_row_selector_cnt_ = 0;
    for (; OB_SUCC(ret) && curr_idx < batch_size; ++curr_idx) {
      if (OB_NOT_NULL(child_skip) && child_skip->at(curr_idx)) {
        my_skip.set(curr_idx);
        continue;
      }
      int64_t curr_pos = -1;
      bool find_bkt = false;
      while (OB_SUCC(ret) && !find_bkt) {
        locate_buckets_[curr_idx] = const_cast<GroupRowBucket *> (&locate_next_bucket(*buckets_, hash_values[curr_idx], curr_pos));
        if (locate_buckets_[curr_idx]->is_valid()) {
          if (probe_by_col) {
            old_row_selector_.at(old_row_selector_cnt_++) = curr_idx;
            __builtin_prefetch(&locate_buckets_[curr_idx]->get_item(),
                          0, 2);
            find_bkt = true;
          } else {
            bool result = true;
            RowItemType *it = &locate_buckets_[curr_idx]->get_item();
            for (int64_t i = 0; OB_SUCC(ret) && result && i < hash_expr_cnt_; ++i) {
              ObIVector *r_vec = gby_exprs_->at(i)->get_vector(*eval_ctx_);
              const bool l_isnull = it->is_null(i);
              const bool r_isnull = r_vec->is_null(curr_idx);
              if (l_isnull != r_isnull) {
                result = false;
              } else if (l_isnull && r_isnull) {
                result = true;
              } else {
                const int64_t l_len = it->get_length(row_meta, i);
                const int64_t r_len = r_vec->get_length(curr_idx);
                if (l_len == r_len && (0 == memcmp(it->get_cell_payload(row_meta, i),
                                                  r_vec->get_payload(curr_idx),
                                                  l_len))) {
                  result = true;
                } else {
                  int cmp_res = 0;
                  if (OB_FAIL(r_vec->null_last_cmp(*gby_exprs_->at(i), curr_idx, false,
                                                  it->get_cell_payload(row_meta, i),
                                                  l_len, cmp_res))) {
                    LOG_WARN("failed to cmp left and right", K(ret));
                  } else {
                    result = (0 == cmp_res);
                  }
                }
              }
            }
            if (OB_SUCC(ret) && result) {
              my_skip.set(curr_idx);
              find_bkt = true;
            }
          }
        } else if (locate_buckets_[curr_idx]->is_occupyed()) {
          batch_duplicate = true;
          find_bkt = true;
        } else {
          //occupy empty bucket
          locate_buckets_[curr_idx]->set_occupyed();
          new_row_selector_.at(new_row_selector_cnt_++) = curr_idx;
          find_bkt = true;
        }
      }
      if (batch_duplicate) {
        break;
      }
    }
    if (OB_SUCC(ret)
      && probe_by_col
      && old_row_selector_cnt_ > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && !need_fallback && i < hash_expr_cnt_; ++i) {
        col_has_null_.at(i) |= gby_exprs_->at(i)->get_vector(*eval_ctx_)->has_null();
        if (col_has_null_.at(i) ?
            OB_FAIL(inner_process_column(*gby_exprs_, row_meta, i, need_fallback))
            : OB_FAIL(inner_process_column_not_null(*gby_exprs_, row_meta, i, need_fallback))) {
          LOG_WARN("failed to process column", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!need_fallback) {
        for (int64_t i = 0; i < old_row_selector_cnt_; ++i) {
          const int64_t idx = old_row_selector_.at(i);
          my_skip.set(idx);
        }
      } else {
        //reset occupyed bkt and stat
        for (int64_t i = 0; i < new_row_selector_cnt_; ++i) {
          locate_buckets_[new_row_selector_.at(i)]->set_empty();
        }
        continue;
      }
    }
    if (OB_FAIL(ret) || 0 == new_row_selector_cnt_) {
    } else if (OB_FAIL(sf(vector_ptrs_, &new_row_selector_.at(0), new_row_selector_cnt_, srows_))) {
      LOG_WARN("failed to append batch", K(ret));
    } else {
      for (int64_t i = 0; i < hash_expr_cnt_; ++i) {
        col_has_null_.at(i) |= gby_exprs_->at(i)->get_vector(*eval_ctx_)->has_null();
      }
      for (int64_t i = 0; i < new_row_selector_cnt_; ++i) {
        int64_t idx = new_row_selector_.at(i);
        locate_buckets_[idx]->set_hash(hash_values[idx]);
        locate_buckets_[idx]->set_valid();
        locate_buckets_[idx]->set_item(static_cast<RowItemType&> (*srows_[i]));
        ++size_;
      }
      new_row_selector_cnt_ = 0;
    }
    processed_idx = curr_idx;
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::likely_equal_nullable(const RowMeta &row_meta,
                                                                const ObCompactRow &left_row,
                                                                const int64_t right_idx,
                                                                bool &result) const
{
  // All rows is in one group, when no group by columns (group by const expr),
  // so the default %result is true
  int ret = OB_SUCCESS;
  result = true;
  for (int64_t i = 0; OB_SUCC(ret) && result && i < hash_expr_cnt_; ++i) {
    bool null_equal = (nullptr == gby_exprs_->at(i));
    if (!null_equal) {
      ObIVector *r_vec = gby_exprs_->at(i)->get_vector(*eval_ctx_);
      const bool l_isnull = left_row.is_null(i);
      const bool r_isnull = r_vec->is_null(right_idx);
      if (l_isnull != r_isnull) {
        result = false;
      } else if (l_isnull && r_isnull) {
        result = true;
      } else {
        const int64_t l_len = left_row.get_length(row_meta, i);
        const int64_t r_len = r_vec->get_length(right_idx);
        if (l_len == r_len && (0 == memcmp(left_row.get_cell_payload(row_meta, i),
                                           r_vec->get_payload(right_idx),
                                           l_len))) {
          result = true;
        } else {
          int cmp_res = 0;
          if (OB_FAIL(r_vec->null_last_cmp(*gby_exprs_->at(i), right_idx, false,
                                           left_row.get_cell_payload(row_meta, i),
                                           l_len, cmp_res))) {
            LOG_WARN("failed to cmp left and right", K(ret));
          } else {
            result = (0 == cmp_res);
          }
        }
      }
    } else {
      result = true;
    }
  }
  return ret;
}

template <typename GroupRowBucket>
void ObExtendHashTableVec<GroupRowBucket>::prefetch(const ObBatchRows &brs, uint64_t *hash_vals) const
{
  if (!sstr_aggr_.is_valid()) {
    auto mask = get_bucket_num() - 1;
    for(auto i = 0; i < brs.size_; i++) {
      if (brs.skip_->at(i)) {
        continue;
      }
      __builtin_prefetch((&buckets_->at((ObGroupRowBucketBase::HASH_VAL_MASK & hash_vals[i]) & mask)),
                          0/* read */, 2 /*high temp locality*/);
    }
  }
}

int BatchAggrRowsTable::init(int64_t max_batch_size, ObIAllocator &alloc/*arena allocator*/)
{
  int ret = OB_SUCCESS;
  max_batch_size_ = max_batch_size;
  if (OB_ISNULL(aggr_rows_ = static_cast<char **> (alloc.alloc(sizeof(char *)
                                                   * MAX_REORDER_GROUPS)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(selectors_ = static_cast<uint16_t **> (alloc.alloc(sizeof(uint16_t *)
                                                              * MAX_REORDER_GROUPS)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(selectors_item_cnt_ = static_cast<uint16_t *> (alloc.alloc(sizeof(uint16_t)
                                                                  * MAX_REORDER_GROUPS)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < MAX_REORDER_GROUPS; ++i) {
      if (OB_ISNULL(selectors_[i] = static_cast<uint16_t *> (alloc.alloc(sizeof(uint16_t)
                                                             * max_batch_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to alloc memory for batch aggr rows table", K(ret), K(max_batch_size_));
  }
  return ret;
}

}//ns sql
}//ns oceanbase
