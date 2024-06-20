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
      if (!std::is_same<Bucket, GenericBucket>::value) {
        void *item_buf = alloc.alloc(sizeof(ItemArray));
        if (OB_ISNULL(bucket_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          if (OB_NOT_NULL(item_buf)) {
            alloc.free(item_buf);
          }
        } else {
          items_ = new (item_buf) ItemArray(*ht_alloc_);
          item_pos_ = 0;
        }
      }
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
  collisions_ = 0;
  used_buckets_ = 0;
  buckets_->reuse();
  OZ (buckets_->init(nbuckets_));
  if (!std::is_same<Bucket, GenericBucket>::value) {
    items_->reuse();
    item_pos_ = 0;
    OZ (items_->init(row_count));
  }

  LOG_DEBUG("build prepare", K(row_count), K(bucket_count), K_(nbuckets), KP(items_), K(sizeof(Bucket)));
  return ret;
}

// Get Item list which has the same hash value.
// return NULL if not found.
template <typename Bucket, typename Prober>
inline typename Bucket::Item *HashTable<Bucket, Prober>::get(const uint64_t hash_val)
{
  uint64_t mask = nbuckets_ - 1;
  uint64_t pos = hash_val & mask;
  typename Bucket::Item *item = reinterpret_cast<typename Bucket::Item *>(END_ITEM);
  Bucket *bucket = &buckets_->at(pos);
  if (bucket->used()) {
    do {
      if (bucket->hash_value_ == hash_val) {
       item = bucket->get_item();
        break;
      }
      // next bucket
      ++bucket;
      ++pos;
      if (OB_UNLIKELY(pos == ((pos >> bit_cnt_) << bit_cnt_) || pos == nbuckets_)) {
        pos = (pos & mask);
        bucket = &buckets_->at(pos);
      }
      // hash table must has empty bucket
      // so we don't judge that the count is greater than bucket number
    } while (bucket->used());
  }
  return item;
}

template <typename Bucket, typename Prober>
void HashTable<Bucket, Prober>::get(uint64_t hash_val, Bucket *&bkt)
{
  Bucket tmp_bucket;
  tmp_bucket.hash_value_ = hash_val;
  uint64_t mask = nbuckets_ - 1;
  uint64_t pos = tmp_bucket.hash_value() & mask;
  bkt = NULL;
  for (int64_t i = 0; i < nbuckets_; i += 1, pos = ((pos + 1) & mask)) {
    Bucket &bucket = buckets_->at(pos);
    if (!bucket.used()) {
      break;
    }
    if (bucket.hash_value() == tmp_bucket.hash_value()) {
      bkt = &bucket;
      break;
    }
  }
}

template <typename Bucket, typename Prober>
void HashTable<Bucket, Prober>::set(JoinTableCtx &ctx,
                                    const uint64_t hash_val,
                                    ObHJStoredRow *row,
                                    int64_t &used_buckets,
                                    int64_t &collisions)
{
  const RowMeta &row_meta = ctx.build_row_meta_;
  Bucket tmp_bucket;
  tmp_bucket.hash_value_ = hash_val;
  uint64_t mask = nbuckets_ - 1;
  uint64_t pos = tmp_bucket.hash_value_ & mask;
  for (int64_t i = 0; i < nbuckets_; i += 1, pos = ((pos + 1) & mask)) {
    Bucket &bucket = buckets_->at(pos);
    if (!bucket.used()) {
      Item *item = NULL;
      if (std::is_same<Item, GenericItem>::value) {
        item = reinterpret_cast<Item *>(row);
        item->init(ctx, row_meta, row, reinterpret_cast<Item *>(END_ITEM));
        bucket.set_item(item);
      } else {
        item = bucket.get_item();
        item->init(ctx, row_meta, row, reinterpret_cast<Item *>(END_ITEM));
      }
      used_buckets += 1;
      bucket.hash_value_ = tmp_bucket.hash_value_;
      bucket.set_used(true);
      break;
    } else if (bucket.hash_value_ == tmp_bucket.hash_value_) {
      Item *old_header = NULL;
      Item *new_header = NULL;
      if (std::is_same<Item, GenericItem>::value) {
        old_header = bucket.get_item();
        new_header = reinterpret_cast<Item *>(row);
        bucket.set_item(new_header);
      } else {
        old_header = new_item();
        *old_header = *bucket.get_item();
        new_header = bucket.get_item();
      }
      //TODO shengle opt, for norimalized bucket, we need not insert new item in bucket,
      // new item can next of bucket item, now we insert new item in bucket just for
      // not caused many case fail, this will opt later
      new_header->init(ctx, row_meta, row, old_header);
      bucket.set_used(true);
      break;
    }
    collisions += 1;
  }
}

// mark delete, can not add row again after delete
template <typename Bucket, typename Prober>
void HashTable<Bucket, Prober>::del(const uint64_t hash_val, const RowMeta &row_meta, Item *item)
{
  Bucket tmp_bucket;
  tmp_bucket.hash_value_ = hash_val;
  uint64_t mask = nbuckets_ - 1;
  uint64_t pos = tmp_bucket.hash_value_ & mask;
  for (int64_t i = 0; i < nbuckets_; i += 1, pos = ((pos + 1) & mask)) {
    Bucket &bucket = buckets_->at(pos);
    if (!bucket.used()) {
      break;
    }
    if (bucket.hash_value_ == tmp_bucket.hash_value_) {
      if (END_ITEM != reinterpret_cast<uint64_t>(bucket.get_item())) {
        if (item == bucket.get_item()) {
          bucket.set_item(item->get_next(row_meta));
          --row_count_;
        } else {
          auto head = bucket.get_item();
          auto s = bucket.get_item()->get_next(row_meta);
          while (END_ITEM != reinterpret_cast<uint64_t>(s)) {
            if (s == item) {
              head->set_next(row_meta, s->get_next(row_meta));
              --row_count_;
              break;
            }
            head = s;
            s = s->get_next(row_meta);
          }
        }
      }
      break;
    }
  }
}

template <typename Bucket, typename Prober>
void HashTable<Bucket, Prober>::reset()
{
  if (OB_NOT_NULL(buckets_)) {
    buckets_->reset();
  }
  if (!std::is_same<Bucket, GenericBucket>::value) {
    if (OB_NOT_NULL(items_)) {
      items_->reset();
    }
  }
  nbuckets_ = 0;
  collisions_ = 0;
  used_buckets_ = 0;
  item_pos_ = 0;
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
  if (OB_NOT_NULL(items_)) {
    items_->destroy();
    alloc->free(items_);
    items_ = nullptr;
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
int HashTable<Bucket, Prober>::insert_batch(JoinTableCtx &ctx,
                                            ObHJStoredRow **stored_rows,
                                            const int64_t size,
                                            int64_t &used_buckets,
                                            int64_t &collisions)
{
  int ret = OB_SUCCESS;
  auto mask = nbuckets_ - 1;
  for (auto i = 0; i < size; i++) {
    __builtin_prefetch((&buckets_->at(stored_rows[i]->get_hash_value(ctx.build_row_meta_) & mask)),
                        1 /* write */, 3 /* high temporal locality*/);
  }
  for (int64_t i = 0; i < size; ++i) {
    set(ctx, stored_rows[i]->get_hash_value(ctx.build_row_meta_),
        stored_rows[i], used_buckets, collisions);
    LOG_DEBUG("build row", K(i), KP(stored_rows[i]),
        "hash_val", stored_rows[i]->get_hash_value(ctx.build_row_meta_),
        "row", ToStrCompactRow(ctx.build_row_meta_, *stored_rows[i], ctx.build_output_),
        "row_meta", ctx.build_row_meta_);
  }

  return ret;
}

template <typename Bucket, typename Prober>
int HashTable<Bucket, Prober>::probe_prepare(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  if (!std::is_same<Item, GenericItem>::value) {
    if (OB_FAIL(ctx.probe_batch_rows_->set_key_data(ctx.probe_keys_,
                                                    ctx.eval_ctx_,
                                                    output_info))) {
      LOG_WARN("fail to init probe keys", K(ret));
    }
  }

  return ret;
}

template <typename Bucket, typename Prober>
int HashTable<Bucket, Prober>::probe_batch_normal(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  if (output_info.first_probe_) {
    uint64_t *hash_vals = ctx.probe_batch_rows_->hash_vals_;
    uint64_t mask = nbuckets_ - 1;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      int64_t hash_val = hash_vals[output_info.selector_[i]];
      __builtin_prefetch(&buckets_->at(hash_val & mask), 0, 1 /*low temporal locality*/);
    }
    int64_t new_selector_cnt = 0;
    int64_t batch_idx = 0;
    Item *item = NULL;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      batch_idx = output_info.selector_[i];
      item = get(hash_vals[batch_idx]);
      OB_ASSERT(NULL != item);
      if (END_ITEM != reinterpret_cast<uint64_t>(item)) {
        ctx.cur_items_[new_selector_cnt] = item;
        output_info.selector_[new_selector_cnt++] = batch_idx;
      }
      LOG_DEBUG("first probe", KP(item), K(i), K(new_selector_cnt), K(batch_idx),
                K(hash_vals[output_info.selector_[i]]), K(output_info.selector_cnt_));
    }
    output_info.selector_cnt_ = new_selector_cnt;
    output_info.first_probe_ = false;
  } else {
    int64_t new_selector_cnt = 0;
    int64_t batch_idx = 0;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      auto item = reinterpret_cast<Item *>(ctx.cur_items_[i]);
      OB_ASSERT(NULL != item);
      if (END_ITEM != reinterpret_cast<uint64_t>(item)) {
        batch_idx = output_info.selector_[i];
        ctx.cur_items_[new_selector_cnt] = item;
        output_info.selector_[new_selector_cnt++] = batch_idx;
      }
      LOG_DEBUG("probe batch", KP(item), K(i), K(new_selector_cnt), K(batch_idx), K(output_info.selector_cnt_), K(ctx.cur_items_[i]));
    }
    output_info.selector_cnt_ = new_selector_cnt;
  }

  if (std::is_same<Item, GenericItem>::value) {
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
        __builtin_prefetch(ctx.cur_items_[i], 0 /* for read */, 3 /* high temporal locality */);
      }
    }
  }

  uint64_t idx = 0;
  //batch_info_guard.set_batch_size(right_brs_->size_);
  if (ctx.probe_opt_) {
    // calc equal condition
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      int64_t batch_idx = output_info.selector_[i];
      Item *item = reinterpret_cast<Item *>(ctx.cur_items_[i]);
      bool matched = false;
      while (!matched && END_ITEM != reinterpret_cast<uint64_t>(item) && OB_SUCC(ret)) {
        ret = prober_.equal(ctx, item, batch_idx, matched);
        if (!matched) {
          item = item->get_next(ctx.build_row_meta_);
        }
        OB_ASSERT(NULL != item);
      } // whild end
      if (matched) {
        // record next iter used item
        ctx.cur_items_[idx] = item->get_next(ctx.build_row_meta_);
        output_info.left_result_rows_[idx] = item->get_stored_row();
        output_info.selector_[idx++] = output_info.selector_[i];
        if (ctx.need_mark_match()) {
          item->set_is_match(ctx.build_row_meta_, true);
        }
      }
    } // for end
  } else {
    LOG_DEBUG("before calc condistions", K(output_info.selector_cnt_));
    for (int64_t i = 0; OB_SUCC(ret) && i < output_info.selector_cnt_; i++) {
      int64_t batch_idx = output_info.selector_[i];
      bool matched = false;
      Item *item = reinterpret_cast<Item *>(ctx.cur_items_[i]);

      while (!matched && END_ITEM != reinterpret_cast<uint64_t>(item) && OB_SUCC(ret)) {
        if (OB_FAIL(prober_.calc_join_conditions(ctx,
                                                 item->get_stored_row(),
                                                 batch_idx,
                                                 matched))) {
          LOG_WARN("fail to calc conditions", K(ret));
        }
        if (!matched) {
          item = item->get_next(ctx.build_row_meta_);
          OB_ASSERT(NULL != item);
        }
      } // while end
      if (matched) {
        ctx.cur_items_[idx] = item->get_next(ctx.build_row_meta_);
        output_info.left_result_rows_[idx] = item->get_stored_row();
        output_info.selector_[idx++] = output_info.selector_[i];
        if (ctx.need_mark_match()) {
          item->set_is_match(ctx.build_row_meta_, true);
        }
      }
    } // for end
  }
  output_info.selector_cnt_ = idx;
  LOG_DEBUG("probe batch", K(idx));

  return ret;
}

template <typename Bucket, typename Prober>
int HashTable<Bucket, Prober>::probe_batch_opt(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  if (output_info.first_probe_) {
    uint64_t *hash_vals = ctx.probe_batch_rows_->hash_vals_;
    uint64_t mask = nbuckets_ - 1;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      int64_t hash_val = hash_vals[output_info.selector_[i]];
      __builtin_prefetch(&buckets_->at(hash_val & mask), 0, 1 /*low temporal locality*/);
    }
    int64_t new_selector_cnt = 0;
    int64_t batch_idx = 0;
    Item *item = NULL;
    bool matched = false;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      batch_idx = output_info.selector_[i];
      item = get(hash_vals[batch_idx]);
      OB_ASSERT(NULL != item);
      while (END_ITEM != reinterpret_cast<uint64_t>(item)) {
        ret = prober_.equal(ctx, item, batch_idx, matched);
        if (matched) {
          output_info.left_result_rows_[new_selector_cnt] = item->get_stored_row();
          ctx.cur_items_[new_selector_cnt] = item->get_next(ctx.build_row_meta_);
          output_info.selector_[new_selector_cnt++] = batch_idx;
          if (ctx.need_mark_match()) {
            item->set_is_match(ctx.build_row_meta_, true);
          }
          break;
        } else {
          item = item->get_next(ctx.build_row_meta_);
        }
      }
      LOG_DEBUG("first probe", KP(item), K(i), K(new_selector_cnt), K(batch_idx),
                K(hash_vals[output_info.selector_[i]]), K(output_info.selector_cnt_));
    }
    output_info.selector_cnt_ = new_selector_cnt;
    output_info.first_probe_ = false;
  } else {
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      if (END_ITEM != reinterpret_cast<uint64_t>(ctx.cur_items_[i])) {
        __builtin_prefetch(ctx.cur_items_[i], 0 /* for read */, 1 /* high temporal locality */);
      }
    }
    int64_t new_selector_cnt = 0;
    int64_t batch_idx = 0;
    for (int64_t i = 0; i < output_info.selector_cnt_; i++) {
      auto item = reinterpret_cast<Item *>(ctx.cur_items_[i]);
      bool matched = false;
      OB_ASSERT(NULL != item);
      batch_idx = output_info.selector_[i];
      while (!matched && END_ITEM != reinterpret_cast<uint64_t>(item) && OB_SUCC(ret)) {
        ret = prober_.equal(ctx, item, batch_idx, matched);
        if (matched) {
          output_info.left_result_rows_[new_selector_cnt] = item->get_stored_row();
          ctx.cur_items_[new_selector_cnt] = item->get_next(ctx.build_row_meta_);
          output_info.selector_[new_selector_cnt++] = batch_idx;
          if (ctx.need_mark_match()) {
            item->set_is_match(ctx.build_row_meta_, true);
          }
        } else {
          item = item->get_next(ctx.build_row_meta_);
        }
      }
      LOG_DEBUG("probe batch", KP(item), K(i), K(new_selector_cnt), K(batch_idx), K(output_info.selector_cnt_), K(ctx.cur_items_[i]));
    }
    output_info.selector_cnt_ = new_selector_cnt;
  }
  return ret;
}

template <typename Bucket, typename Prober>
int HashTable<Bucket, Prober>::probe_batch_del_match(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  Item *item = reinterpret_cast<Item *>(END_ITEM);
  int64_t result_idx = 0;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*ctx.eval_ctx_);
  batch_info_guard.set_batch_size(ctx.probe_batch_rows_->brs_.size_);
  for (int64_t i = 0; OB_SUCC(ret) && i < output_info.selector_cnt_; i++) {
    Bucket *bkt = nullptr;
    get(ctx.probe_batch_rows_->hash_vals_[output_info.selector_[i]], bkt);
    if (NULL != bkt) {
      item = bkt->used() ? bkt->get_item() : reinterpret_cast<Item *>(END_ITEM);
    }
    if (END_ITEM != reinterpret_cast<uint64_t>(item)) {
      ctx.cur_items_[idx] = item;
      output_info.selector_[idx++] = output_info.selector_[i];

      int64_t batch_idx = output_info.selector_[i];
      batch_info_guard.set_batch_idx(batch_idx);
      bool matched = false;
      Item *pre = reinterpret_cast<Item *>(END_ITEM);
      while (!matched && END_ITEM != reinterpret_cast<uint64_t>(item) && OB_SUCC(ret)) {
        if (OB_FAIL(prober_.calc_join_conditions(ctx,
                                                 item->get_stored_row(),
                                                 batch_idx,
                                                 matched))) {
          LOG_WARN("fail to calc conditions", K(ret));
        }
        if (!matched) {
          pre = item;
          item = item->get_next(ctx.build_row_meta_);
        }
      }
      if (matched) {
        LOG_DEBUG("trace match", K(ret), K(batch_idx));
        ctx.cur_items_[result_idx] = item->get_next(ctx.build_row_meta_);
        output_info.selector_[result_idx] = output_info.selector_[i];
        output_info.left_result_rows_[result_idx++] = item->get_stored_row();
        if (reinterpret_cast<Item *>(END_ITEM) == pre) {
          bkt->set_item(item->get_next(ctx.build_row_meta_));
        } else {
          pre->set_next(ctx.build_row_meta_, item->get_next(ctx.build_row_meta_));
        }
        row_count_ -= 1;
      }
    }
  }
  output_info.selector_cnt_ = result_idx;

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

template <typename Bucket, typename Prober>
int HashTable<Bucket, Prober>::get_unmatched_rows(JoinTableCtx &ctx, OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  Item *item = reinterpret_cast<Item *>(ctx.cur_tuple_);
  int64_t batch_idx = 0;
  while (OB_SUCC(ret) && batch_idx < *ctx.max_output_cnt_) {
    if (END_ITEM != reinterpret_cast<uint64_t>(item)) {
      if (!item->is_match(ctx.build_row_meta_)) {
        output_info.left_result_rows_[batch_idx] = item->get_stored_row();
        batch_idx++;
      }
      item = item->get_next(ctx.build_row_meta_);
    } else {
      int64_t bucket_id = ctx.cur_bkid_ + 1;
      if (bucket_id < nbuckets_) {
        Bucket &bkt = buckets_->at(bucket_id);
        item = bkt.used() ? bkt.get_item() : reinterpret_cast<Item *>(END_ITEM);
        ctx.cur_bkid_ = bucket_id;
      } else {
        ret = OB_ITER_END;
      }
    }
  }
  output_info.selector_cnt_ = batch_idx;
  ctx.cur_tuple_ = item;

  return ret;
}

template<typename Item>
int ProberBase<Item>::calc_join_conditions(JoinTableCtx &ctx,
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

inline int GenericSharedHashTable::insert_batch(JoinTableCtx &ctx,
                                         ObHJStoredRow **stored_rows,
                                         const int64_t size,
                                         int64_t &used_buckets,
                                         int64_t &collisions)
{
  int ret = OB_SUCCESS;
  auto mask = nbuckets_ - 1;
  for (auto i = 0; i < size; i++) {
    __builtin_prefetch((&buckets_->at(stored_rows[i]->get_hash_value(ctx.build_row_meta_) & mask)),
                        1 , 3);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    ret = atomic_set(stored_rows[i]->get_hash_value(ctx.build_row_meta_),
                     ctx.build_row_meta_,
                     reinterpret_cast<Item *>(stored_rows[i]),
                     used_buckets,
                     collisions);
  }

  return ret;
}

inline int GenericSharedHashTable::atomic_set(const uint64_t hash_val,
                                       const RowMeta &row_meta,
                                       GenericItem *item,
                                       int64_t &used_buckets,
                                       int64_t &collisions)
{
  int ret = OB_SUCCESS;
  GenericBucket new_bucket;
  new_bucket.hash_value_ = hash_val;
  new_bucket.used_ = true;
  new_bucket.set_item(item);
  uint64_t mask = nbuckets_ - 1;
  uint64_t pos = new_bucket.hash_value_ & mask;
  bool added = false;
  GenericBucket old_bucket;
  uint64_t old_val;
  uint64_t old_item;
  for (int64_t i = 0; i < nbuckets_; i += 1, pos = ((pos + 1) & mask)) {
    GenericBucket &bucket = buckets_->at(pos);
    do {
      old_val = ATOMIC_LOAD(&bucket.val_);
      old_bucket.val_ = old_val;
      if (!old_bucket.used_) {
        if (ATOMIC_BCAS(&bucket.val_, old_val, new_bucket.val_)) {
          // write hash_value and used_ flag successfully
          // then write item
          ++used_buckets;
          old_item = ATOMIC_LOAD(&bucket.item_ptr_);
          item->set_next(row_meta, reinterpret_cast<Item *>(END_ITEM));
          if (ATOMIC_BCAS(&bucket.item_ptr_, old_item, reinterpret_cast<uint64_t>(new_bucket.item_ptr_))) {
            added = true;
          }
        }
      } else if (old_val == new_bucket.val_) {
        old_item = ATOMIC_LOAD(&bucket.item_ptr_);
        if (0 == old_item) {
          // do nothing
        } else {
          item->set_next(row_meta, reinterpret_cast<Item *>(old_item));
          if (ATOMIC_BCAS(&bucket.item_ptr_, old_item, new_bucket.item_ptr_)) {
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
    ++collisions;
  }
  LOG_DEBUG("atomic set", K(this), K(collisions), K(used_buckets));

  return ret;
}

template <typename Bucket, typename Prober>
inline int NormalizedSharedHashTable<Bucket, Prober>::insert_batch(JoinTableCtx &ctx,
                                                   ObHJStoredRow **stored_rows,
                                                   const int64_t size,
                                                   int64_t &used_buckets,
                                                   int64_t &collisions)
{
  int ret = OB_SUCCESS;
  auto mask = this->nbuckets_ - 1;
  for (auto i = 0; i < size; i++) {
    __builtin_prefetch((&this->buckets_->at(stored_rows[i]->get_hash_value(ctx.build_row_meta_) & mask)),
                        1 /* write */, 3 /* high temporal locality*/);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    ret = atomic_set(ctx, stored_rows[i]->get_hash_value(ctx.build_row_meta_),
                     stored_rows[i],
                     used_buckets,
                     collisions);

  }

  return ret;
}

template <typename Bucket, typename Prober>
inline int NormalizedSharedHashTable<Bucket, Prober>::atomic_set(JoinTableCtx &ctx,
                                       const uint64_t hash_val,
                                       ObHJStoredRow *sr,
                                       int64_t &used_buckets,
                                       int64_t &collisions)
{
  int ret = OB_SUCCESS;
  Bucket new_bucket;
  new_bucket.hash_value_ = hash_val;
  new_bucket.used_ = true;
  const RowMeta &row_meta = ctx.build_row_meta_;
  uint64_t mask = this->nbuckets_ - 1;
  uint64_t pos = new_bucket.hash_value_ & mask;
  bool added = false;
  Bucket old_bucket;
  uint64_t old_val;
  for (int64_t i = 0; i < this->nbuckets_; i += 1, pos = ((pos + 1) & mask)) {
    Bucket &bucket = this->buckets_->at(pos);
    do {
      old_val = ATOMIC_LOAD(&bucket.val_);
      old_bucket.val_ = old_val;
      if (!old_bucket.used_) {
        // write hash_value and used_ flag
        if (ATOMIC_BCAS(&bucket.val_, old_val, new_bucket.val_)) {
          // then write next item
          ++used_buckets;
          bucket.item_.init(ctx, row_meta,  sr, reinterpret_cast<Item *>(END_ITEM));
          added = true;
        }
      } else if (old_val == new_bucket.val_) {
        uint64_t old_item = ATOMIC_LOAD(&bucket.item_.next_item_ptr_);
        if (0 == old_item) {
          // do nothing
        } else {
          Item *new_item = this->atomic_new_item();
          new_item->init(ctx, row_meta,  sr, reinterpret_cast<Item *>(END_ITEM));
          new_item->set_next(row_meta, reinterpret_cast<Item *>(old_item));
          if (ATOMIC_BCAS(&bucket.item_.next_item_ptr_, old_item, reinterpret_cast<uint64_t>(new_item))) {
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
    ++collisions;
  }
  LOG_DEBUG("atomic set", K(this), K(collisions), K(used_buckets));

  return ret;
}

} // end namespace sql
} // end namespace oceanbase
