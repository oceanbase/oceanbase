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
int ObExtendHashTableVec<GroupRowBucket>::get(const int64_t batch_idx, uint64_t hash_val, char *&aggr_row)
{
  aggr_row = NULL;
  int ret = OB_SUCCESS;
  bool result = false;
  locate_bucket_ = nullptr;
  ++probe_cnt_;
  locate_bucket_ = const_cast<GroupRowBucket *> (&locate_bucket(*buckets_, hash_val));
  if (locate_bucket_->is_valid()) {
    ObGroupRowItemVec *it = &locate_bucket_->get_item();
    while (OB_SUCC(ret) && nullptr != it) {
      if (OB_FAIL((this->*likely_equal_function_)(group_store_.get_row_meta(), *it, batch_idx, result))) {
        LOG_WARN("failed to cmp", K(ret));
      } else if (result) {
        aggr_row = it->get_aggr_row(group_store_.get_row_meta());
        break;
      }
      if (OB_SUCC(ret)) {
        if (nullptr != it->next(group_store_.get_row_meta())) {
          it = it->next(group_store_.get_row_meta());
        } else {
          break;
        }
      }
    }
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::append(const common::ObIArray<ObExpr *> &gby_exprs,
                                                 const int64_t batch_idx,
                                                 const uint64_t hash_value,
                                                 const ObIArray<int64_t> &lengths,
                                                 char *&get_row,
                                                 bool need_reinit_vectors /*false*/)
{
  int ret = OB_SUCCESS;
  ObCompactRow *srow = nullptr;
  uint16_t idx = static_cast<uint16_t> (batch_idx);
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
  if (OB_FAIL(group_store_.add_batch(vector_ptrs_, &idx, 1, &srow, &lengths))) {
    LOG_WARN("failed to add store row", K(ret), K(idx));
  } else if (auto_extend_ && OB_UNLIKELY(size_ * SIZE_BUCKET_SCALE >= get_bucket_num())) {
    if (OB_FAIL(extend())) {
      SQL_ENG_LOG(WARN, "extend failed", K(ret));
    } else {
      //relocate bucket
      locate_bucket_ = const_cast<GroupRowBucket *>(&locate_bucket(*buckets_, hash_value));
    }
  }
  LOG_DEBUG("append new row", "new row",
           CompactRow2STR(group_store_.get_row_meta(), *srow, &gby_exprs));
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(locate_bucket_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get locate bucket", K(ret));
    } else if (!locate_bucket_->is_valid()) {
      locate_bucket_->set_hash(hash_value);
      locate_bucket_->set_valid();
      locate_bucket_->set_bkt_seq(size_);
      static_cast<ObGroupRowItemVec &> (srow[0]).set_next(nullptr, group_store_.get_row_meta());
      locate_bucket_->set_item(static_cast<ObGroupRowItemVec &> (srow[0]));
      get_row = locate_bucket_->get_item().get_aggr_row(group_store_.get_row_meta());
      CK (OB_NOT_NULL(get_row));
    } else {
      ObGroupRowItemVec *new_item = static_cast<ObGroupRowItemVec *> (&srow[0]);
      new_item->set_next(locate_bucket_->get_item().next(group_store_.get_row_meta()),
                         group_store_.get_row_meta());
      locate_bucket_->get_item().set_next(new_item, group_store_.get_row_meta());
      get_row = new_item->get_aggr_row(group_store_.get_row_meta());
      CK (OB_NOT_NULL(get_row));
    }
    size_ += 1;
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
    GroupRowBucket *bucket = const_cast<GroupRowBucket *> (&locate_bucket(*buckets_, hash_values[i]));
    if (!bucket->is_valid()) {
      bucket->set_hash(hash_values[i]);
      bucket->set_valid();
      bucket->set_bkt_seq(size_);
      static_cast<ObGroupRowItemVec &> (srow).set_next(nullptr, group_store_.get_row_meta());
      bucket->set_item(static_cast<ObGroupRowItemVec &> (srow));
    } else {
      ObGroupRowItemVec *new_item = static_cast<ObGroupRowItemVec *> (&srow);
      new_item->set_next(bucket->get_item().next(group_store_.get_row_meta()),
                         group_store_.get_row_meta());
      bucket->get_item().set_next(new_item, group_store_.get_row_meta());
    }
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
      } else if (!curr_bkt->is_valid()) {
        curr_bkt->set_hash(hash_values[new_row_selector_.at(i)]);
        curr_bkt->set_valid();
        curr_bkt->set_bkt_seq(size_ + i);
        static_cast<ObGroupRowItemVec &> (*curr_row).set_next(nullptr, group_store_.get_row_meta());
        curr_bkt->set_item(static_cast<ObGroupRowItemVec &> (*curr_row));
        get_row = curr_bkt->get_item().get_aggr_row(group_store_.get_row_meta());
        CK (OB_NOT_NULL(get_row));
      } else {
        ObGroupRowItemVec *new_item = static_cast<ObGroupRowItemVec *> (curr_row);
        new_item->set_next(curr_bkt->get_item().next(group_store_.get_row_meta()), group_store_.get_row_meta());
        curr_bkt->get_item().set_next(new_item, group_store_.get_row_meta());
        get_row = new_item->get_aggr_row(group_store_.get_row_meta());
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
    int64_t curr_idx = 0;
    bool hash_crash = false;
    while (OB_SUCC(ret) && !hash_crash && curr_idx < child_brs.size_) {
      bool batch_duplicate = false;
      new_row_selector_cnt_ = 0;
      for (; OB_SUCC(ret) && !batch_duplicate && !hash_crash && curr_idx < child_brs.size_; ++curr_idx) {
        if (child_brs.skip_->at(curr_idx)
            || is_dumped[curr_idx]
            || (nullptr != bloom_filter
                && !bloom_filter->exist(ObGroupRowBucketBase::HASH_VAL_MASK & hash_values[curr_idx]))) {
          continue;
        }
        locate_buckets_[curr_idx] = const_cast<GroupRowBucket *> (&locate_bucket(*buckets_, hash_values[curr_idx]));
        if (locate_buckets_[curr_idx]->is_valid()) {
          ++probe_cnt_;
          ++agg_row_cnt;
          bool result = false;
          ObGroupRowItemVec *it = &locate_buckets_[curr_idx]->get_item();
          while (OB_SUCC(ret) && nullptr != it) {
            if (OB_FAIL((this->*likely_equal_function_)(group_store_.get_row_meta(), *it, curr_idx, result))) {
              LOG_WARN("failed to cmp", K(ret));
            } else if (result) {
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
              break;
            }
            if (OB_SUCC(ret)) {
              if (nullptr != it->next(group_store_.get_row_meta())) {
                it = it->next(group_store_.get_row_meta());
              } else {
                break;
              }
            }
          }
          if (!batch_old_rows[curr_idx]) {
            //hash value crash, can not add batch
            hash_crash = true;
            if (batch_aggr_rows && batch_aggr_rows->is_valid()) {
              batch_aggr_rows->set_invalid();
            }
            break;
          }
        } else if (locate_buckets_[curr_idx]->is_occupyed()) {
          batch_duplicate = true;
          break;
        } else if (can_append_batch) {
          //occupy empty bucket
          locate_buckets_[curr_idx]->set_occupyed();
          new_row_selector_.at(new_row_selector_cnt_++) = curr_idx;
          ++probe_cnt_;
          ++agg_row_cnt;
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
          new_row_selector_cnt_ = 0;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (hash_crash) {
      int64_t real_batch_size = child_brs.size_;
      const_cast<ObBatchRows &> (child_brs).size_ = curr_idx;
      if (can_append_batch
          && new_row_selector_cnt_ > 0
          && OB_FAIL(append_batch(gby_exprs, child_brs, is_dumped, hash_values,
                                lengths, batch_new_rows, agg_group_cnt,
                                need_reinit_vectors))) {
        LOG_WARN("failed to append batch", K(ret));
      }
      const_cast<ObBatchRows &> (child_brs).size_ = real_batch_size;
      // then get && append 1 by 1
      --probe_cnt_;
      --agg_row_cnt;
      for (; OB_SUCC(ret) && curr_idx < child_brs.size_; ++curr_idx) {
        if (child_brs.skip_->at(curr_idx) || is_dumped[curr_idx]) {
          continue;
        }
        char *aggr_row = nullptr;
        char *new_row = nullptr;
        if (OB_FAIL(get(curr_idx, hash_values[curr_idx], aggr_row))) {
          LOG_WARN("failed to get row", K(ret));
        } else if (NULL != aggr_row) {
          ++agg_row_cnt;
          batch_old_rows[curr_idx] = aggr_row;
        } else if (can_append_batch) {
          ++agg_row_cnt;
          ++agg_group_cnt;
          if (OB_FAIL(append(gby_exprs, curr_idx, hash_values[curr_idx],
                              lengths, new_row, need_reinit_vectors))) {
            LOG_WARN("failed to append new item", K(ret));
          } else {
            batch_new_rows[curr_idx] = new_row;
          }
        }
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
    change_valid_idx_.set_allocator(allocator);
    if (use_sstr_aggr && OB_FAIL(sstr_aggr_.init(allocator_, *eval_ctx, gby_exprs, aggr_row_size))) {
      LOG_WARN("failed to init short string aggr", K(ret));
    } else if (OB_FAIL(vector_ptrs_.prepare_allocate(gby_exprs.count()))) {
      SQL_ENG_LOG(WARN, "failed to alloc ptrs", K(ret));
    } else if (OB_FAIL(new_row_selector_.prepare_allocate(max_batch_size))) {
      SQL_ENG_LOG(WARN, "failed to alloc array", K(ret));
    } else if (OB_ISNULL(buckets_buf = allocator_.alloc(sizeof(BucketArray), mem_attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
    } else {
      if (!nullable && all_int64) {
        likely_equal_function_ = &ObExtendHashTableVec<GroupRowBucket>::likely_equal_fixed64;
      } else if (!nullable) {
        likely_equal_function_ = &ObExtendHashTableVec<GroupRowBucket>::likely_equal;
      } else if (all_int64) {
        likely_equal_function_ = &ObExtendHashTableVec<GroupRowBucket>::likely_equal_fixed64_nullable;
      } else {
        likely_equal_function_ = &ObExtendHashTableVec<GroupRowBucket>::likely_equal_nullable;
      }
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
int ObExtendHashTableVec<GroupRowBucket>::set_distinct(const RowMeta &row_meta,
                                                       const uint16_t batch_idx,
                                                       uint64_t hash_value,
                                                       StoreRowFunc sf)
{
  int ret = OB_SUCCESS;
  GroupRowBucket *bucket = nullptr;
  if (OB_UNLIKELY(NULL == buckets_)) {
    // do nothing
  } else if (auto_extend_ && OB_UNLIKELY(size_ * SIZE_BUCKET_SCALE >= get_bucket_num())) {
    if (OB_FAIL(extend())) {
      SQL_ENG_LOG(WARN, "failed to extend hash table", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    bool result = false;
    bool need_insert = true;
    ObCompactRow *srow = nullptr;
    if (!is_inited_vec_) {
      for (int64_t i = 0; i < gby_exprs_->count(); ++i) {
        vector_ptrs_.at(i) = gby_exprs_->at(i)->get_vector(*eval_ctx_);
      }
      is_inited_vec_ = true;
    }
    bucket = const_cast<GroupRowBucket *> (&locate_bucket(*buckets_, hash_value));
    if (bucket->is_valid()) {
      RowItemType *it = &(bucket->get_item());
      while (OB_SUCC(ret) && nullptr != it) {
        if (OB_FAIL((this->*likely_equal_function_)(row_meta, static_cast<ObCompactRow&>(*it), batch_idx, result))) {
          LOG_WARN("failed to cmp", K(ret));
        } else if (result) {
          need_insert = false;
          ret = OB_HASH_EXIST;
          LOG_DEBUG("entry exist", K(ret));
          break;
        }
        it = it->next(row_meta);
      }
      if (OB_SUCC(ret) && need_insert) {
        if (OB_FAIL(sf(vector_ptrs_, &batch_idx, 1, &srow))) {
          LOG_WARN("failed to store row", K(ret));
        } else {
          RowItemType *new_item = static_cast<RowItemType *> (&srow[0]);
          new_item->set_next(bucket->get_item().next(row_meta), row_meta);
          bucket->get_item().set_next(new_item, row_meta);
          ++size_;
        }
      }
    } else {
      if (OB_FAIL(sf(vector_ptrs_, &batch_idx, 1, &srow))) {
        LOG_WARN("failed to store row", K(ret));
      } else {
        bucket->set_hash(hash_value);
        bucket->set_valid();
        bucket->set_item(static_cast<RowItemType&> (srow[0]));
        ++size_;
      }
    }
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
  if (auto_extend_ && OB_UNLIKELY((size_ + batch_size) * SIZE_BUCKET_SCALE >= get_bucket_num())) {
    if (OB_FAIL(extend())) {
      SQL_ENG_LOG(WARN, "extend failed", K(ret));
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
    int64_t curr_idx = 0;
    bool hash_crash = false;
    while (OB_SUCC(ret) && !hash_crash && curr_idx < batch_size) {
      bool batch_duplicate = false;
      new_row_selector_cnt_ = 0;
      for (; OB_SUCC(ret) && !batch_duplicate && !hash_crash && curr_idx < batch_size; ++curr_idx) {
        if (OB_NOT_NULL(child_skip) && child_skip->at(curr_idx)) {
          my_skip.set(curr_idx);
          continue;
        }
        locate_buckets_[curr_idx] = const_cast<GroupRowBucket *> (&locate_bucket(*buckets_, hash_values[curr_idx]));
        if (locate_buckets_[curr_idx]->is_valid()) {
          bool result = false;
          RowItemType *it = &(locate_buckets_[curr_idx]->get_item());
          while (OB_SUCC(ret) && nullptr != it) {
            if (OB_FAIL((this->*likely_equal_function_)(row_meta, static_cast<ObCompactRow&>(*it), curr_idx, result))) {
              LOG_WARN("failed to cmp", K(ret));
            } else if (result) {
              my_skip.set(curr_idx);
              break;
            }
            it = it->next(row_meta);
          }
          if (!result) {
            hash_crash = true;
            break;
          }
        } else if (locate_buckets_[curr_idx]->is_occupyed()) {
          batch_duplicate = true;
          break;
        } else {
          //occupy empty bucket
          locate_buckets_[curr_idx]->set_occupyed();
          new_row_selector_.at(new_row_selector_cnt_++) = curr_idx;
        }
      }
      if (OB_FAIL(ret) || 0 == new_row_selector_cnt_) {
      } else if (OB_FAIL(sf(vector_ptrs_, &new_row_selector_.at(0), new_row_selector_cnt_, srows_))) {
        LOG_WARN("failed to append batch", K(ret));
      } else {
        for (int64_t i = 0; i < new_row_selector_cnt_; ++i) {
          int64_t idx = new_row_selector_.at(i);
          locate_buckets_[idx]->set_hash(hash_values[idx]);
          locate_buckets_[idx]->set_valid();
          locate_buckets_[idx]->set_item(static_cast<RowItemType&> (*srows_[i]));
          ++size_;
        }
        new_row_selector_cnt_ = 0;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (hash_crash) {
      if (new_row_selector_cnt_ > 0) {
        // first append remain rows
        if (OB_FAIL(sf(vector_ptrs_, &new_row_selector_.at(0), new_row_selector_cnt_, srows_))) {
          LOG_WARN("failed to append batch", K(ret));
        } else {
           for (int64_t i = 0; i < new_row_selector_cnt_; ++i) {
            int64_t idx = new_row_selector_.at(i);
            locate_buckets_[idx]->set_hash(hash_values[idx]);
            locate_buckets_[idx]->set_valid();
            locate_buckets_[idx]->set_item(static_cast<RowItemType&> (*srows_[i]));
            ++size_;
          }
        }
      }
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
      batch_info_guard.set_batch_size(batch_size);
      // then get && append 1 by 1
      for (; OB_SUCC(ret) && curr_idx < batch_size; ++curr_idx) {
        if (OB_NOT_NULL(child_skip) && child_skip->at(curr_idx)) {
          my_skip.set(curr_idx);
          continue;
        }
        batch_info_guard.set_batch_idx(curr_idx);
        if (OB_FAIL(set_distinct(row_meta, curr_idx, hash_values[curr_idx], sf))) {
          if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
            my_skip.set(curr_idx);
          } else {
            SQL_ENG_LOG(WARN, "failed to set distinct value into hash table", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::likely_equal_fixed64(const RowMeta &row_meta,
                                                               const ObCompactRow &left_row,
                                                               const int64_t right_idx,
                                                               bool &result) const
{
  // All rows is in one group, when no group by columns (group by const expr),
  // so the default %result is true
  int ret = OB_SUCCESS;
  result = true;
  const int64_t fixed_length = 8;
  for (int64_t i = 0; OB_SUCC(ret) && result && i < hash_expr_cnt_; ++i) {
    bool null_equal = (nullptr == gby_exprs_->at(i));
    if (!null_equal) {
      ObIVector *r_vec = gby_exprs_->at(i)->get_vector(*eval_ctx_);
      if (0 == memcmp(left_row.get_cell_payload(row_meta, i),
                      r_vec->get_payload(right_idx),
                      fixed_length)) {
        result = true;
      } else {
        int cmp_res = 0;
        if (OB_FAIL(r_vec->null_last_cmp(*gby_exprs_->at(i), right_idx, false,
                                          left_row.get_cell_payload(row_meta, i),
                                          fixed_length, cmp_res))) {
          LOG_WARN("failed to cmp left and right", K(ret));
        } else {
          result = (0 == cmp_res);
        }
      }
    } else {
      result = true;
    }
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::likely_equal_fixed64_nullable(const RowMeta &row_meta,
                                                                        const ObCompactRow &left_row,
                                                                        const int64_t right_idx,
                                                                        bool &result) const
{
  // All rows is in one group, when no group by columns (group by const expr),
  // so the default %result is true
  int ret = OB_SUCCESS;
  result = true;
  const int64_t fixed_length = 8;
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
      } else if (0 == memcmp(left_row.get_cell_payload(row_meta, i),
                      r_vec->get_payload(right_idx),
                      fixed_length)) {
        result = true;
      } else {
        int cmp_res = 0;
        if (OB_FAIL(r_vec->null_last_cmp(*gby_exprs_->at(i), right_idx, false,
                                          left_row.get_cell_payload(row_meta, i),
                                          fixed_length, cmp_res))) {
          LOG_WARN("failed to cmp left and right", K(ret));
        } else {
          result = (0 == cmp_res);
        }
      }
    } else {
      result = true;
    }
  }
  return ret;
}

template <typename GroupRowBucket>
int ObExtendHashTableVec<GroupRowBucket>::likely_equal(const RowMeta &row_meta,
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
    } else {
      result = true;
    }
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
    if (ObGroupRowBucketType::OUTLINE == GroupRowBucket::TYPE) {
      for(auto i = 0; i < brs.size_; i++) {
        if (brs.skip_->at(i)) {
          continue;
        }
        __builtin_prefetch(&buckets_->at((ObGroupRowBucketBase::HASH_VAL_MASK & hash_vals[i]) & mask).get_item(),
                          0, 2 );
      }
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
