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
#include "share/vector/ob_fixed_length_vector.h"
#include "share/vector/ob_continuous_vector.h"
#include "share/vector/ob_uniform_vector.h"
#include "share/vector/ob_discrete_vector.h"
#include "lib/atomic/ob_atomic.h"
#include "share/vector/expr_cmp_func.h"

namespace oceanbase
{
namespace sql
{

template <typename Bucket, typename Prober>
int HashTable<Bucket, Prober>::init(ObIAllocator &alloc, const int64_t max_batch_size) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    void *alloc_buf = alloc.alloc(sizeof(ModulePageAllocator));
    void *bucket_buf = alloc.alloc(sizeof(BucketArray));
    if (OB_ISNULL(bucket_buf) || OB_ISNULL(alloc_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      if (OB_NOT_NULL(bucket_buf)) {
        alloc.free(bucket_buf);
      }
      if (OB_NOT_NULL(alloc_buf)) {
        alloc.free(alloc_buf);
      }
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      ht_alloc_ = new (alloc_buf) ModulePageAllocator(alloc);
      ht_alloc_->set_label("HtOpAlloc");
      buckets_ = new (bucket_buf) BucketArray(*ht_alloc_);
      magic_ = MAGIC_CODE;
      bit_cnt_ = __builtin_ctz(BucketArray::BLOCK_CAPACITY);
      inited_ = true;
    }
  }

  return ret;
}

template <typename Bucket, typename Prober>
int HashTable<Bucket, Prober>::build_prepare(int64_t row_count, int64_t bucket_count)
{
  int ret = OB_SUCCESS;
  row_count_ = row_count;
  nbuckets_ = std::max(nbuckets_, bucket_count);
  bucket_mask_ = nbuckets_ - 1;
  collisions_ = 0;
  used_buckets_ = 0;
  buckets_->reuse();
  OZ (buckets_->init(nbuckets_));
  LOG_DEBUG("build prepare", K(row_count), K(bucket_count), K_(nbuckets), K(sizeof(Bucket)));
  return ret;
}

template <typename Bucket, typename Prober>
void HashTable<Bucket, Prober>::reset()
{
  if (OB_NOT_NULL(buckets_)) {
    buckets_->reset();
  }
  nbuckets_ = 0;
  collisions_ = 0;
  used_buckets_ = 0;
}

template <typename Bucket, typename Prober>
void HashTable<Bucket, Prober>::free(ObIAllocator *alloc)
{
  reset();
  if (OB_NOT_NULL(buckets_)) {
    buckets_->destroy();
    alloc->free(buckets_);
    buckets_ = nullptr;
  }
  if (OB_NOT_NULL(ht_alloc_)) {
    ht_alloc_->reset();
    ht_alloc_->~ModulePageAllocator();
    alloc->free(ht_alloc_);
    ht_alloc_ = nullptr;
  }
  inited_ = false;
}

template <typename Bucket, typename Prober>
int HashTable<Bucket, Prober>::set(
    JoinTableCtx &ctx, ObHJStoredRow *row_ptr, int64_t &used_buckets, int64_t &collisions)
{
  int ret = OB_SUCCESS;
  const RowMeta &row_meta = ctx.build_row_meta_;
  uint64_t hash_val = row_ptr->get_hash_value(row_meta);
  uint64_t salt = Bucket::extract_salt(hash_val);
  uint64_t pos = hash_val & bucket_mask_;
  Bucket *bucket = nullptr;
  bool equal = false;
  for (int64_t i = 0; i < nbuckets_ && OB_SUCC(ret); i++, pos = (pos + 1) & bucket_mask_) {
    bucket = &buckets_->at(pos);
    if (!bucket->used()) {
      bucket->init(ctx, salt, row_ptr);
      row_ptr->set_next(row_meta, reinterpret_cast<ObHJStoredRow *>(END_ROW_PTR));
      used_buckets++;
      break;
    } else if (salt == bucket->get_salt()) {
      ObHJStoredRow *left_ptr = bucket->get_stored_row();
      if (OB_FAIL(key_equal(ctx, left_ptr, row_ptr, equal))) {
        LOG_WARN("key equal error", K(ret));
      } else if (equal) {
        row_ptr->set_next(row_meta, left_ptr);
        bucket->set_row_ptr(row_ptr);
        break;
      }
    }
    collisions++;
  }
  LOG_DEBUG("insert row", KP(row_ptr), KP(bucket), K(pos), K(hash_val), K(salt),
        "row", ToStrCompactRow(ctx.build_row_meta_, *row_ptr, ctx.build_output_),
        "row_meta", ctx.build_row_meta_);
  return ret;
}

template <typename Bucket, typename Prober>
int HashTable<Bucket, Prober>::insert_batch(JoinTableCtx &ctx, ObHJStoredRow **stored_rows,
    const int64_t size, int64_t &used_buckets, int64_t &collisions)
{
  int ret = OB_SUCCESS;
  for (auto i = 0; i < size; i++) {
    __builtin_prefetch(
        (&buckets_->at(stored_rows[i]->get_hash_value(ctx.build_row_meta_) & bucket_mask_)),
        1 /* write */,
        3 /* high temporal locality*/);
  }
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < size; ++row_idx) {
    ObHJStoredRow *row_ptr = stored_rows[row_idx];
    if (OB_FAIL(set(ctx, row_ptr, used_buckets, collisions))) {
      LOG_WARN("set row error", K(ret));
    }
  }
  return ret;
}

template <typename Bucket, typename Prober>
int HashTable<Bucket, Prober>::key_equal(
    JoinTableCtx &ctx, ObHJStoredRow *left_row, ObHJStoredRow *right_row, bool &equal)
{
  // use for comparing keys of row in building hashtable
  int ret = OB_SUCCESS;
  equal = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.build_key_proj_->count(); i++) {
    int64_t build_col_idx = ctx.build_key_proj_->at(i);
    bool left_is_null = left_row->is_null(build_col_idx);
    bool right_is_null = right_row->is_null(build_col_idx);
    const char *l_v = NULL;
    const char *r_v = NULL;
    ObLength l_len = 0;
    ObLength r_len = 0;
    int cmp_ret = 0;
    left_row->get_cell_payload(ctx.build_row_meta_, build_col_idx, l_v, l_len);
    right_row->get_cell_payload(ctx.build_row_meta_, build_col_idx, r_v, r_len);
    NullSafeRowCmpFunc cmp_func = ctx.build_cmp_funcs_.at(i);
    // Need to be concerned about character-set
    if (OB_FAIL((cmp_func)(ctx.build_keys_->at(i)->obj_meta_,
                    ctx.build_keys_->at(i)->obj_meta_,
                    (const void *)l_v,
                    l_len,
                    left_is_null,
                    (const void *)r_v,
                    r_len,
                    right_is_null,
                    cmp_ret))) {
      LOG_WARN("row_cmp_func failed!", K(ret));
    } else if (cmp_ret != 0) {
      equal = false;
      break;
    }
  }
  return ret;
}

template <typename Bucket, typename Prober>
int HashTable<Bucket, Prober>::probe_prepare(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  if (std::is_same<Bucket, NormalizedBucket<Int64Key>>::value
      || std::is_same<Bucket, NormalizedBucket<Int128Key>>::value
      || std::is_same<Bucket, NormalizedRobinBucket<Int64Key>>::value
      || std::is_same<Bucket, NormalizedRobinBucket<Int128Key>>::value) {
    if (OB_FAIL(ctx.probe_batch_rows_->set_key_data(ctx.probe_keys_, ctx.eval_ctx_, output_info))) {
      LOG_WARN("fail to init probe keys", K(ret));
    }
  }
  return ret;
}

template <typename Bucket>
int ProberBase<Bucket>::calc_other_join_conditions_batch(
    JoinTableCtx &ctx, const ObHJStoredRow **rows, const uint16_t *sel, const uint16_t sel_cnt)
{
  int ret = OB_SUCCESS;
  int64_t batch_idx;
  bool all_rows_active = (sel_cnt == ctx.probe_batch_rows_->brs_.size_);
  if (all_rows_active) {
    ctx.eval_skip_->reset(ctx.probe_batch_rows_->brs_.size_);
    for (uint16_t i = 0; i < sel_cnt; ++i) {
      batch_idx = sel[i];
      ctx.clear_one_row_eval_flag(batch_idx);
      ctx.join_cond_matched_[i] = true;
    }
  } else {
    ctx.eval_skip_->set_all(ctx.probe_batch_rows_->brs_.size_);
    for (uint16_t i = 0; i < sel_cnt; ++i) {
      batch_idx = sel[i];
      ctx.clear_one_row_eval_flag(batch_idx);
      ctx.eval_skip_->unset(batch_idx);
      ctx.join_cond_matched_[i] = true;
    }
  }

  if (OB_FAIL(ObHJStoredRow::convert_rows_to_exprs(
          *ctx.build_output_, *ctx.eval_ctx_, ctx.build_row_meta_, rows, sel, sel_cnt))) {
    LOG_WARN("failed to convert expr", K(ret));
  } else {
    const ExprFixedArray *conds = ctx.other_conds_;
    uint16_t remaining_cnt = sel_cnt;
    ARRAY_FOREACH(*conds, i)
    {
      if (0 == remaining_cnt) {
        break;
      }
      ObExpr *expr = conds->at(i);
      if (OB_FAIL(expr->eval_vector(*ctx.eval_ctx_,
              *ctx.eval_skip_,
              ctx.probe_batch_rows_->brs_.size_,
              all_rows_active))) {
        LOG_WARN("fail to eval vector", K(ret));
      } else {
        if (is_uniform_format(expr->get_format(*ctx.eval_ctx_))) {
          ObUniformBase *uni_vec = static_cast<ObUniformBase *>(expr->get_vector(*ctx.eval_ctx_));
          for (int64_t j = 0; j < sel_cnt; ++j) {
            batch_idx = sel[j];
            if (ctx.eval_skip_->at(batch_idx)) {
              continue;
            }
            if ((uni_vec->is_null(batch_idx) || 0 == uni_vec->get_int(batch_idx))) {
              ctx.join_cond_matched_[j] = false;
              ctx.eval_skip_->set(batch_idx);
              all_rows_active = false;
              remaining_cnt--;
            }
          }
        } else {
          ObFixedLengthBase *fixed_vec =
              static_cast<ObFixedLengthBase *>(expr->get_vector(*ctx.eval_ctx_));
          for (int64_t j = 0; j < sel_cnt; ++j) {
            batch_idx = sel[j];
            if (ctx.eval_skip_->at(batch_idx)) {
              continue;
            }
            if ((fixed_vec->is_null(batch_idx) || 0 == fixed_vec->get_int(batch_idx))) {
              ctx.join_cond_matched_[j] = false;
              ctx.eval_skip_->set(batch_idx);
              all_rows_active = false;
              remaining_cnt--;
            }
          }
        }
      }
    }  // for end
  }
  return ret;
}

template <typename Bucket, typename Prober>
int HashTable<Bucket, Prober>::probe_batch_normal(
    JoinTableCtx &ctx, OutputInfo &output_info, bool has_other_conds)
{
  // first, find a batch row in hashtable where salt is same ==> salt_match_rows
  // second, salt_match_rows ==> key_match_rows and key_unmatch_rows
  // do while key_unmatch_rows is empty
  // third, key_match_rows ==> other_conds_match_rows and other_conds_unmatch_rows
  // do while other_conds_unmatch_rows is empty
  int ret = OB_SUCCESS;
  if (output_info.first_probe_) {
    uint64_t *hash_vals = ctx.probe_batch_rows_->hash_vals_;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      int64_t hash_val = hash_vals[output_info.selector_[i]];
      ctx.unmatched_pos_[i] = hash_val & bucket_mask_;
      ctx.unmatched_sel_[i] = output_info.selector_[i];
      __builtin_prefetch(&buckets_->at(hash_val & bucket_mask_), 0, 1);
    }
    // new_selector_cnt: size of bucket is key matched
    // unmatched_cnt: size of buckets is key unmatched
    uint16_t new_selector_cnt = 0;
    uint16_t unmatched_cnt = output_info.selector_cnt_;
    while (OB_SUCC(ret) && unmatched_cnt > 0) {
      // get batch buckets compare key with probe rows, cmp result is in cmp_ret_map
      find_batch(ctx, unmatched_cnt, false);
      if (OB_FAIL(prober_.equal_batch(ctx, ctx.unmatched_sel_, unmatched_cnt, false))) {
        LOG_WARN("probe equal batch failed", K(ret), K(unmatched_cnt));
      }
      uint16_t unmatched_idx = 0;
      for (int64_t i = 0; i < unmatched_cnt; i++) {
        uint16_t batch_idx = ctx.unmatched_sel_[i];
        if (0 == ctx.cmp_ret_map_[i]) {
          ObHJStoredRow *row_ptr = ctx.unmatched_rows_[i];
          if (!has_other_conds) {
            // has no other conds, row is macthed
            ctx.scan_chain_rows_[new_selector_cnt] = row_ptr->get_next(ctx.build_row_meta_);
            output_info.left_result_rows_[new_selector_cnt] = row_ptr;
            if (ctx.need_mark_match()) {
              row_ptr->set_is_match(ctx.build_row_meta_, true);
            }
          } else {
            ctx.scan_chain_rows_[new_selector_cnt] = row_ptr;
          }
          output_info.selector_[new_selector_cnt++] = batch_idx;
        } else {
          uint64_t pos = ctx.unmatched_pos_[i];
          ctx.unmatched_sel_[unmatched_idx] = batch_idx;
          // get next bucket pos to compare
          ctx.unmatched_pos_[unmatched_idx++] = (pos + 1) & bucket_mask_;
        }
      }
      unmatched_cnt = unmatched_idx;
    }
    output_info.selector_cnt_ = new_selector_cnt;
    output_info.first_probe_ = false;
  } else {
    int64_t new_selector_cnt = 0;
    int64_t batch_idx = 0;
    ObHJStoredRow *row_ptr = NULL;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      row_ptr = ctx.scan_chain_rows_[i];
      if (END_ROW_PTR == reinterpret_cast<uint64_t>(row_ptr)) {
        continue;
      }
      batch_idx = output_info.selector_[i];
      if (!has_other_conds) {
        output_info.left_result_rows_[new_selector_cnt] = row_ptr;
        ctx.scan_chain_rows_[new_selector_cnt] = row_ptr->get_next(ctx.build_row_meta_);
        if (ctx.need_mark_match()) {
          row_ptr->set_is_match(ctx.build_row_meta_, true);
        }
      } else {
        ctx.scan_chain_rows_[new_selector_cnt] = row_ptr;
      }
      output_info.selector_[new_selector_cnt++] = batch_idx;
      LOG_DEBUG("probe batch normal",
          K(i),
          K(new_selector_cnt),
          K(batch_idx),
          K(output_info.selector_cnt_));
    }
    output_info.selector_cnt_ = new_selector_cnt;

    if (has_other_conds) {
      for (int64_t i = 0; i < new_selector_cnt; i++) {
        __builtin_prefetch(&ctx.scan_chain_rows_[i], 0, 3);
      }
    }
  }

  // calc other join conditions
  if (has_other_conds) {
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      ctx.unmatched_rows_[i] = ctx.scan_chain_rows_[i];
      ctx.unmatched_sel_[i] = output_info.selector_[i];
    }
    uint64_t idx = 0;
    uint64_t unmatched_idx = 0;
    uint64_t unmatched_cnt = output_info.selector_cnt_;
    LOG_DEBUG("before calc condistions", K(unmatched_cnt));
    while (OB_SUCC(ret) && 0 < unmatched_cnt) {
      unmatched_idx = 0;
      // need to ensure unmatched_rows are not END_ROW_PTR!
      if (OB_FAIL(prober_.calc_other_join_conditions_batch(ctx,
              const_cast<const ObHJStoredRow **>(ctx.unmatched_rows_),
              ctx.unmatched_sel_,
              unmatched_cnt))) {
        LOG_WARN("fail to calc conditions", K(ret));
      } else {
        for (int64_t i = 0; i < unmatched_cnt; i++) {
          int64_t batch_idx = ctx.unmatched_sel_[i];
          ObHJStoredRow *row_ptr = ctx.unmatched_rows_[i];
          ObHJStoredRow *next_row_ptr = row_ptr->get_next(ctx.build_row_meta_);
          if (ctx.join_cond_matched_[i]) {
            ctx.scan_chain_rows_[idx] = next_row_ptr;
            output_info.selector_[idx] = batch_idx;
            output_info.left_result_rows_[idx++] = row_ptr;
            if (ctx.need_mark_match()) {
              row_ptr->set_is_match(ctx.build_row_meta_, true);
            }
          } else {
            if (END_ROW_PTR != reinterpret_cast<uint64_t>(next_row_ptr)) {
              ctx.unmatched_rows_[unmatched_idx] = next_row_ptr;
              ctx.unmatched_sel_[unmatched_idx++] = batch_idx;
            }
          }
        }
      }
      unmatched_cnt = unmatched_idx;
    }
    output_info.selector_cnt_ = idx;
    LOG_DEBUG("probe batch normal", K(idx));
  } else if (OB_FAIL(ObHJStoredRow::convert_rows_to_exprs(*ctx.build_output_,
                 *ctx.eval_ctx_,
                 ctx.build_row_meta_,
                 output_info.left_result_rows_,
                 output_info.selector_,
                 output_info.selector_cnt_))) {
    LOG_WARN("failed to convert expr", K(ret));
  }
  return ret;
}

template <typename Bucket, typename Prober>
void HashTable<Bucket, Prober>::find_batch(
    JoinTableCtx &ctx, uint16_t &unmatched_cnt, bool is_del_matched)
{
  // to find batch rows in buckets where salt is matched.
  Bucket *bucket = NULL;
  uint64_t pos;
  uint64_t *hash_vals = ctx.probe_batch_rows_->hash_vals_;
  uint16_t unmatched_idx = 0;
  for (int64_t i = 0; i < unmatched_cnt; i++) {
    uint16_t batch_idx = ctx.unmatched_sel_[i];
    uint64_t hash_val = hash_vals[batch_idx];
    uint64_t salt = Bucket::extract_salt(hash_val);
    pos = ctx.unmatched_pos_[i];
    for (int j = 0; j < nbuckets_; j += 1, pos = ((pos + 1) & bucket_mask_)) {
      bucket = &buckets_->at(pos);
      if (!bucket->used()) {
        bucket = NULL;
        break;
      } else if (salt == bucket->get_salt()) {
        if (is_del_matched && END_ROW_PTR == reinterpret_cast<uint64_t>(bucket->get_stored_row())) {
          continue;
        }
        ctx.unmatched_sel_[unmatched_idx] = batch_idx;
        ctx.unmatched_bkts_[unmatched_idx] = bucket;
        ctx.unmatched_rows_[unmatched_idx] = bucket->get_stored_row();
        ctx.unmatched_pos_[unmatched_idx++] = pos;
        break;
      }
    }
  }
  unmatched_cnt = unmatched_idx;

  if (std::is_same<Bucket, GenericBucket>::value || std::is_same<Bucket, RobinBucket>::value) {
    for (int64 i = 0; i < unmatched_cnt; i++) {
      __builtin_prefetch(&ctx.unmatched_rows_[i], 0, 3);
    }
  }
}

template <typename Bucket, typename Prober>
int HashTable<Bucket, Prober>::probe_batch_opt(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  if (output_info.first_probe_) {
    uint64_t *hash_vals = ctx.probe_batch_rows_->hash_vals_;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      uint64_t hash_val = hash_vals[output_info.selector_[i]];
      ctx.unmatched_pos_[i] = hash_val & bucket_mask_;
      ctx.unmatched_sel_[i] = output_info.selector_[i];
      __builtin_prefetch(&buckets_->at(hash_val & bucket_mask_), 0, 3);
    }
    uint16_t new_selector_cnt = 0;
    uint16_t unmatched_cnt = output_info.selector_cnt_;
    while (OB_SUCC(ret) && unmatched_cnt > 0) {
      find_batch(ctx, unmatched_cnt, false);
      if (OB_FAIL(prober_.equal_batch(ctx, ctx.unmatched_sel_, unmatched_cnt, true))) {
        LOG_WARN("probe equal batch failed", K(ret), K(unmatched_cnt));
      }
      uint16_t unmatched_idx = 0;
      for (int64_t i = 0; i < unmatched_cnt; i++) {
        uint16_t batch_idx = ctx.unmatched_sel_[i];
        if (0 == ctx.cmp_ret_map_[i]) {
          ObHJStoredRow *row_ptr = ctx.unmatched_rows_[i];
          output_info.left_result_rows_[new_selector_cnt] = row_ptr;
          ctx.scan_chain_rows_[new_selector_cnt] = row_ptr->get_next(ctx.build_row_meta_);
          output_info.selector_[new_selector_cnt++] = batch_idx;
          if (ctx.need_mark_match()) {
            row_ptr->set_is_match(ctx.build_row_meta_, true);
          }
        } else {
          uint64_t pos = ctx.unmatched_pos_[i];
          ctx.unmatched_sel_[unmatched_idx] = batch_idx;
          ctx.unmatched_pos_[unmatched_idx++] = (pos + 1) & bucket_mask_;
        }
      }
      unmatched_cnt = unmatched_idx;
    }
    output_info.selector_cnt_ = new_selector_cnt;
    output_info.first_probe_ = false;
  } else {
    int64_t new_selector_cnt = 0;
    int64_t batch_idx = 0;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      batch_idx = output_info.selector_[i];
      ObHJStoredRow *build_row_ptr = ctx.scan_chain_rows_[i];
      if (END_ROW_PTR == reinterpret_cast<uint64_t>(build_row_ptr)) {
        continue;
      }
      output_info.left_result_rows_[new_selector_cnt] = build_row_ptr;
      ctx.scan_chain_rows_[new_selector_cnt] = build_row_ptr->get_next(ctx.build_row_meta_);
      output_info.selector_[new_selector_cnt++] = batch_idx;
      if (ctx.need_mark_match()) {
        build_row_ptr->set_is_match(ctx.build_row_meta_, true);
      }
      LOG_DEBUG(
          "probe batch opt", K(i), K(new_selector_cnt), K(batch_idx), K(output_info.selector_cnt_));
    }
    output_info.selector_cnt_ = new_selector_cnt;
  }
  return ret;
}

template <typename Bucket, typename Prober>
int HashTable<Bucket, Prober>::probe_batch_del_match(
    JoinTableCtx &ctx, OutputInfo &output_info, bool has_other_conds)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  int64_t result_idx = 0;
  if (output_info.first_probe_) {
    uint64_t *hash_vals = ctx.probe_batch_rows_->hash_vals_;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      uint64_t hash_val = hash_vals[output_info.selector_[i]];
      ctx.unmatched_pos_[i] = hash_val & bucket_mask_;
      ctx.unmatched_sel_[i] = output_info.selector_[i];
      __builtin_prefetch(&buckets_->at(hash_val & bucket_mask_), 0, 1);
    }
    uint16_t new_selector_cnt = 0;
    uint16_t unmatched_cnt = output_info.selector_cnt_;
    while (OB_SUCC(ret) && unmatched_cnt > 0) {
      find_batch(ctx, unmatched_cnt, true);
      if(OB_FAIL(prober_.equal_batch(ctx, ctx.unmatched_sel_, unmatched_cnt, false))) {
        LOG_WARN("probe equal batch failed", K(ret), K(unmatched_cnt));
      }
      uint16_t unmatched_idx = 0;
      for (int64_t i = 0; i < unmatched_cnt; i++) {
        uint16_t batch_idx = ctx.unmatched_sel_[i];
        if (0 == ctx.cmp_ret_map_[i]) {
          ObHJStoredRow *row_ptr = ctx.unmatched_rows_[i];
          if (!has_other_conds) {
            if (!row_ptr->is_match(ctx.build_row_meta_)) {
              ObHJStoredRow *next_row_ptr = row_ptr->get_next(ctx.build_row_meta_);
              ctx.del_matched_bkts_[new_selector_cnt] = ctx.unmatched_bkts_[i];
              ctx.scan_chain_rows_[new_selector_cnt] = next_row_ptr;
              output_info.left_result_rows_[new_selector_cnt] = row_ptr;
              output_info.selector_[new_selector_cnt++] = batch_idx;

              // delete from bucket list
              reinterpret_cast<Bucket *>(ctx.unmatched_bkts_[i])->set_row_ptr(next_row_ptr);
              row_ptr->set_is_match(ctx.build_row_meta_, true);
              row_count_ -= 1;
            }
          } else {
            ctx.del_bkts_[new_selector_cnt] = ctx.unmatched_bkts_[i];
            ctx.del_pre_rows_[new_selector_cnt] = reinterpret_cast<ObHJStoredRow *>(END_ROW_PTR);
            ctx.del_rows_[new_selector_cnt] = row_ptr;
            ctx.del_sel_[new_selector_cnt++] = batch_idx;
          }
        } else {
          uint64_t pos = ctx.unmatched_pos_[i];
          ctx.unmatched_sel_[unmatched_idx] = batch_idx;
          ctx.unmatched_pos_[unmatched_idx++] = (pos + 1) & bucket_mask_;
        }
      }
      unmatched_cnt = unmatched_idx;
    }
    output_info.selector_cnt_ = new_selector_cnt;
    output_info.first_probe_ = false;
  } else {
    int64_t new_selector_cnt = 0;
    int64_t batch_idx = 0;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      batch_idx = output_info.selector_[i];
      ObHJStoredRow *row_ptr = ctx.scan_chain_rows_[i];
      if (END_ROW_PTR == reinterpret_cast<uint64_t>(row_ptr)) {
        continue;
      }
      if (!has_other_conds) {
        if (!row_ptr->is_match(ctx.build_row_meta_)) {
          ObHJStoredRow *next_row_ptr = row_ptr->get_next(ctx.build_row_meta_);
          ctx.del_matched_bkts_[new_selector_cnt] = ctx.del_matched_bkts_[i];
          ctx.scan_chain_rows_[new_selector_cnt] = next_row_ptr;
          output_info.left_result_rows_[new_selector_cnt] = row_ptr;
          output_info.selector_[new_selector_cnt++] = batch_idx;

          // delete from bucket list
          reinterpret_cast<Bucket *>(ctx.del_matched_bkts_[i])->set_row_ptr(next_row_ptr);
          row_ptr->set_is_match(ctx.build_row_meta_, true);
          row_count_ -= 1;
        }
      } else {
        ctx.del_bkts_[new_selector_cnt] = ctx.del_matched_bkts_[i];
        ctx.del_pre_rows_[new_selector_cnt] = ctx.del_matched_pre_rows_[i];
        ctx.del_rows_[new_selector_cnt] = ctx.scan_chain_rows_[i];
        ctx.del_sel_[new_selector_cnt++] = batch_idx;
      }
      LOG_DEBUG(
          "probe del batch", K(i), K(new_selector_cnt), K(batch_idx), K(output_info.selector_cnt_));
    }
    output_info.selector_cnt_ = new_selector_cnt;

    if (has_other_conds) {
      for (int64_t i = 0; i < new_selector_cnt; i++) {
        __builtin_prefetch(&ctx.del_rows_[i], 0, 3);
      }
    }
  }

  if (has_other_conds) {
    int64_t idx = 0;
    int64_t unmatched_idx = 0;
    int64_t unmatched_cnt = output_info.selector_cnt_;
    while (OB_SUCC(ret) && 0 < unmatched_cnt) {
      // ensure ctx.del_rows_ have no END_ROW
      if (OB_FAIL(prober_.calc_other_join_conditions_batch(ctx,
              const_cast<const ObHJStoredRow **>(ctx.del_rows_),
              ctx.del_sel_,
              unmatched_cnt))) {
        LOG_WARN("fail to calc conditions", K(ret));
      } else {
        unmatched_idx = 0;
        for (uint16_t i = 0; i < unmatched_cnt; i++) {
          uint16_t batch_idx = ctx.del_sel_[i];
          ObHJStoredRow *row_ptr = ctx.del_rows_[i];
          ObHJStoredRow *next_row_ptr = row_ptr->get_next(ctx.build_row_meta_);
          if (!ctx.join_cond_matched_[i]) {
            if (END_ROW_PTR != reinterpret_cast<uint64_t>(next_row_ptr)) {
              ctx.del_bkts_[unmatched_idx] = ctx.del_bkts_[i];
              ctx.del_pre_rows_[unmatched_idx] = row_ptr;
              ctx.del_rows_[unmatched_idx] = next_row_ptr;
              ctx.del_sel_[unmatched_idx++] = batch_idx;
            }
          } else {
            if (!row_ptr->is_match(ctx.build_row_meta_)) {
              ctx.del_matched_bkts_[idx] = ctx.del_bkts_[i];
              ctx.del_matched_pre_rows_[idx] = ctx.del_pre_rows_[i];
              ctx.scan_chain_rows_[idx] = next_row_ptr;
              output_info.left_result_rows_[idx] = row_ptr;
              output_info.selector_[idx++] = batch_idx;
              // avoid duplicate match in one batch
              row_ptr->set_is_match(ctx.build_row_meta_, true);
              ObHJStoredRow *prev_row = ctx.del_pre_rows_[i];
              if (END_ROW_PTR == reinterpret_cast<uint64_t>(prev_row)) {
                reinterpret_cast<Bucket *>(ctx.del_bkts_[i])->set_row_ptr(next_row_ptr);
              } else {
                prev_row->set_next(ctx.build_row_meta_, next_row_ptr);
              }
              row_count_ -= 1;
            } else {
              if (END_ROW_PTR != reinterpret_cast<uint64_t>(next_row_ptr)) {
                ctx.del_bkts_[unmatched_idx] = ctx.del_bkts_[i];
                ctx.del_pre_rows_[unmatched_idx] = ctx.del_pre_rows_[i];
                ctx.del_rows_[unmatched_idx] = next_row_ptr;
                ctx.del_sel_[unmatched_idx++] = batch_idx;
              }
            }

          }
        }
        unmatched_cnt = unmatched_idx;
      }
    }
    output_info.selector_cnt_ = idx;
  } else if (OB_FAIL(ObHJStoredRow::convert_rows_to_exprs(*ctx.build_output_,
                 *ctx.eval_ctx_,
                 ctx.build_row_meta_,
                 output_info.left_result_rows_,
                 output_info.selector_,
                 output_info.selector_cnt_))) {
    LOG_WARN("failed to convert expr", K(ret));
  }
  return ret;
}

template <typename Bucket, typename Prober>
int HashTable<Bucket, Prober>::project_matched_rows(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHJStoredRow::attach_rows(*ctx.build_output_,
                                         *ctx.eval_ctx_,
                                         ctx.build_row_meta_,
                                         output_info.left_result_rows_,
                                         output_info.selector_,
                                         output_info.selector_cnt_))) {
    LOG_WARN("fail to attach rows",  K(ret));
  }

  return ret;
}

template<typename Bucket>
int ProberBase<Bucket>::calc_join_conditions(JoinTableCtx &ctx,
                                           ObHJStoredRow *left_row,
                                           const int64_t batch_idx,
                                           bool &matched) {
  int ret = OB_SUCCESS;
  matched = true;
  ctx.clear_one_row_eval_flag(batch_idx);
  if (OB_FAIL(ObHJStoredRow::convert_one_row_to_exprs(*ctx.build_output_,
                                                      *ctx.eval_ctx_,
                                                      ctx.build_row_meta_,
                                                      left_row,
                                                      batch_idx))) {
    LOG_WARN("failed to convert expr", K(ret));
  } else {
    const ExprFixedArray *conds = ctx.join_conds_;
    ARRAY_FOREACH(*conds, i) {
      ObExpr *expr = conds->at(i);
      if (OB_FAIL(expr->eval_vector(*ctx.eval_ctx_,
                                    *ctx.probe_batch_rows_->brs_.skip_,
                                    EvalBound(ctx.probe_batch_rows_->brs_.size_,
                                              batch_idx, batch_idx + 1, true)))) {
        LOG_WARN("fail to eval vector", K(ret));
      } else {
        if (is_uniform_format(expr->get_format(*ctx.eval_ctx_))) {
          ObUniformBase *uni_vec = static_cast<ObUniformBase *>(
                                                  expr->get_vector(*ctx.eval_ctx_));
          if (uni_vec->is_null(batch_idx) || 0 == uni_vec->get_int(batch_idx)) {
            matched = false;
            break;
          }
        } else {
          ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(
                                                  expr->get_vector(*ctx.eval_ctx_));
          if (fixed_vec->is_null(batch_idx) || 0 == fixed_vec->get_int(batch_idx)) {
            matched = false;
            break;
          }
        }
      }
    } // for end

    LOG_DEBUG("trace match", K(ret), K(matched), K(batch_idx));
  }

  return ret;
}

template <typename Bucket, typename Prober>
void RobinHashTable<Bucket, Prober>::set_and_shift_up(JoinTableCtx &ctx, ObHJStoredRow *row_ptr, const RowMeta &row_meta, uint64_t salt, uint64_t dist, uint64_t pos)
{
  Bucket bucket;
  bucket.init(ctx, salt, row_ptr);
  bucket.set_dist(dist);
  row_ptr->set_next(row_meta, reinterpret_cast<ObHJStoredRow *>(END_ROW_PTR));
  for (int64_t i = 0; i < this->nbuckets_; i++, pos = ((pos + 1) & this->bucket_mask_)) {
    Bucket &cur_bucket = this->buckets_->at(pos);
    if (!cur_bucket.used()) {
      cur_bucket = bucket;
      cur_bucket.inc_dist();
      break;
    }
    cur_bucket.inc_dist();
    std::swap(cur_bucket, bucket);
  }
}

template <typename Bucket, typename Prober>
int RobinHashTable<Bucket, Prober>::insert_batch(JoinTableCtx &ctx, ObHJStoredRow **stored_rows,
    const int64_t size, int64_t &used_buckets, int64_t &collisions)
{
  int ret = OB_SUCCESS;
  const RowMeta &row_meta = ctx.build_row_meta_;

  // prefetch bucket
  for (int64_t i = 0; i < size; i++) {
    uint64_t pos = stored_rows[i]->get_hash_value(row_meta) & this->bucket_mask_;
    __builtin_prefetch(&this->buckets_->at(pos), 1 /* write */, 3 /* high temporal locality*/);
  }

  // set row_ptr
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < size; row_idx++) {
    ObHJStoredRow *row_ptr = stored_rows[row_idx];
    uint64_t hash_val = row_ptr->get_hash_value(row_meta);
    uint64_t salt = Bucket::extract_salt(hash_val);
    uint64_t pos = hash_val & this->bucket_mask_;
    uint64_t dist = 0;
    Bucket *bucket = NULL;
    bool equal = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < this->nbuckets_; i++, pos = ((pos + 1) & this->bucket_mask_)) {
      bucket = &(this->buckets_->at(pos));
      if (!bucket->used()) {
        bucket->init(ctx, salt, row_ptr);
        bucket->set_dist(dist);
        row_ptr->set_next(row_meta, reinterpret_cast<ObHJStoredRow *>(END_ROW_PTR));
        used_buckets++;
        break;
      } else if (salt == bucket->get_salt()) {
        ObHJStoredRow *left_ptr = bucket->get_stored_row();
        if (OB_FAIL(this->key_equal(ctx, left_ptr, row_ptr, equal))) {
          LOG_WARN("key equal error", K(ret));
        } else if (equal) {
          row_ptr->set_next(row_meta, left_ptr);
          bucket->set_row_ptr(row_ptr);
          break;
        } else if (dist > bucket->get_dist()) {
          set_and_shift_up(ctx, row_ptr, row_meta, salt, dist, pos);
          break;
        }
      } else {
        ObHJStoredRow *left_ptr = bucket->get_stored_row();
        if (dist > bucket->get_dist()) {
          set_and_shift_up(ctx, row_ptr, row_meta, salt, dist, pos);
          break;
        }
      }
      dist++;
      collisions++;
    }
    LOG_DEBUG("build row", K(row_idx), KP(stored_rows[row_idx]),
    "hash_val", hash_val, K(used_buckets),
    "row", ToStrCompactRow(ctx.build_row_meta_, *stored_rows[row_idx], ctx.build_output_),
    "row_meta", ctx.build_row_meta_);
  }
  return ret;
}

int GenericSharedHashTable::insert_batch(JoinTableCtx &ctx,
                                         ObHJStoredRow **stored_rows,
                                         const int64_t size,
                                         int64_t &used_buckets,
                                         int64_t &collisions)
{
  int ret = OB_SUCCESS;
  const RowMeta &row_meta = ctx.build_row_meta_;
  for (auto i = 0; i < size; i++) {
    __builtin_prefetch(&(this->buckets_->at(stored_rows[i]->get_hash_value(row_meta) & this->bucket_mask_)),
                        1 , 3);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    ret = atomic_set(ctx,
                    stored_rows[i]->get_hash_value(row_meta),
                     row_meta,
                     stored_rows[i],
                     used_buckets,
                     collisions);
  }

  return ret;
}

inline int GenericSharedHashTable::atomic_set(JoinTableCtx &ctx, const uint64_t hash_val,
    const RowMeta &row_meta, ObHJStoredRow *row_ptr, int64_t &used_buckets, int64_t &collisions)
{
  int ret = OB_SUCCESS;
  uint64_t salt = GenericBucket::extract_salt(hash_val);
  uint64_t pos = hash_val & this->bucket_mask_;
  GenericBucket *bucket;
  GenericBucket old_bucket, new_bucket;
  new_bucket.set_salt(salt);
  new_bucket.set_row_ptr(row_ptr);
  bool equal = false;
  bool added = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < this->nbuckets_; i += 1, pos = ((pos + 1) & this->bucket_mask_)) {
    bucket = &(this->buckets_->at(pos));
    do {
      old_bucket.val_ = ATOMIC_LOAD(&bucket->val_);
      if (!old_bucket.used()) {
        row_ptr->set_next(row_meta, reinterpret_cast<ObHJStoredRow *>(END_ROW_PTR));
        if (ATOMIC_BCAS(&bucket->val_, old_bucket.val_, new_bucket.val_)) {
          added = true;
          used_buckets++;
        }
      } else if (salt == old_bucket.get_salt()) {
        ObHJStoredRow *left_ptr = reinterpret_cast<ObHJStoredRow *>(old_bucket.get_stored_row());
        if (OB_FAIL(this->key_equal(ctx, left_ptr, row_ptr, equal))) {
          LOG_WARN("key equal error", K(ret));
        } else {
          if (!equal) {
            break;
          }
          row_ptr->set_next(row_meta, left_ptr);
          if (ATOMIC_BCAS(&bucket->val_, old_bucket.val_, new_bucket.val_)) {
            added = true;
          }
        }
      } else {
        break;
      }
    } while (OB_SUCC(ret) && !added);
    if (added) {
      break;
    }
    collisions++;
  }
  LOG_DEBUG("insert row", KP(row_ptr), KP(bucket), K(pos), K(hash_val), K(salt), K(added),
        "row", ToStrCompactRow(ctx.build_row_meta_, *row_ptr, ctx.build_output_),
        "row_meta", ctx.build_row_meta_);

  return ret;
}

template <typename T>
int NormalizedSharedHashTable<T>::insert_batch(JoinTableCtx &ctx,
                                                   ObHJStoredRow **stored_rows,
                                                   const int64_t size,
                                                   int64_t &used_buckets,
                                                   int64_t &collisions)
{
  int ret = OB_SUCCESS;
  const RowMeta &row_meta = ctx.build_row_meta_;
  OB_ASSERT(ctx.build_key_proj_->count() == 1);
  for (auto i = 0; i < size; i++) {
    __builtin_prefetch(&(this->buckets_->at(stored_rows[i]->get_hash_value(row_meta) & this->bucket_mask_)),
                        1 , 3);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    ret = atomic_set(ctx,
                     stored_rows[i]->get_hash_value(row_meta),
                     stored_rows[i],
                     used_buckets,
                     collisions);
  }
  return ret;
}

template <typename T>
inline int NormalizedSharedHashTable<T>::atomic_set(JoinTableCtx &ctx, const uint64_t hash_val,
    ObHJStoredRow *row_ptr, int64_t &used_buckets, int64_t &collisions)
{
  int ret = OB_SUCCESS;
  const RowMeta &row_meta = ctx.build_row_meta_;
  uint64_t salt = Bucket::extract_salt(hash_val);
  uint64_t pos = hash_val & this->bucket_mask_;
  Bucket *bucket;
  Bucket old_bucket, new_bucket;
  new_bucket.init(ctx, salt, row_ptr);
  bool equal = false;
  bool added = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < this->nbuckets_; i += 1, pos = ((pos + 1) & this->bucket_mask_)) {
    bucket = &(this->buckets_->at(pos));
    do {
      old_bucket.val_ = ATOMIC_LOAD(&bucket->val_);
      if (!old_bucket.used()) {
        row_ptr->set_next(row_meta, reinterpret_cast<ObHJStoredRow *>(END_ROW_PTR));
        if (ATOMIC_BCAS(&bucket->val_, old_bucket.val_, new_bucket.val_)) {
          MEMCPY(&bucket->key_, &new_bucket.key_, sizeof(T));
          added = true;
          used_buckets++;
        }
      } else if (salt == old_bucket.get_salt()) {
        ObHJStoredRow *left_ptr = old_bucket.get_stored_row();
        if (OB_FAIL(this->key_equal(ctx, left_ptr, row_ptr, equal))) {
          LOG_WARN("key equal error", K(ret));
        } else {
          if (!equal) {
            break;
          }
          row_ptr->set_next(row_meta, left_ptr);
          if (ATOMIC_BCAS(&bucket->val_, old_bucket.val_, new_bucket.val_)) {
            added = true;
          }
        }
      } else {
        break;
      }
    } while (!added);
    if (added) {
      break;
    }
    collisions++;
  }
  LOG_DEBUG("insert row", KP(row_ptr), KP(bucket), K(pos), K(hash_val), K(salt), K(added),
        "row", ToStrCompactRow(ctx.build_row_meta_, *row_ptr, ctx.build_output_),
        "row_meta", ctx.build_row_meta_);

  return ret;
}

} // end namespace sql
} // end namespace oceanbase
