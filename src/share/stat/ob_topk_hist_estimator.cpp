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
#include "ob_topk_hist_estimator.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include <stdint.h>

namespace oceanbase
{
namespace common
{

struct Compare
{
  Compare(): ret_(OB_SUCCESS) {}

  bool operator()(const ObTopkItem &l, const ObTopkItem &r)
  {
    bool less = false;
    int &ret = ret_;
    if (OB_FAIL(ret)) {
      // already fail
    } else if (OB_UNLIKELY(l.col_obj_.is_null()) ||
               OB_UNLIKELY(r.col_obj_.is_null())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else {
      less = l.fre_times_ > r.fre_times_;//Descending sort
    }
    return less;
  }

  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(Compare);
};

struct CopyableComparer
{
  CopyableComparer(Compare &compare) : compare_(compare) {}
  bool operator()(const ObTopkItem &l, const ObTopkItem &r)
  {
    return compare_(l, r);
  }
  Compare &compare_;
};

//sort and reserve topk sort
int ObTopKFrequencyHistograms::create_topk_fre_items(const ObObjMeta *obj_meta/*default null*/)
{
  int ret = OB_SUCCESS;
  topk_fre_items_.set_allocator(&topk_buf_);
  if (OB_FAIL(topk_fre_items_.prepare_allocate(used_list_.count()))) {
    LOG_WARN("failed to init fixed array", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < used_list_.count(); ++i) {
    if (is_need_merge_topk_hist()) {
      ObTopkItem *topk_item = NULL;
      if (OB_ISNULL(topk_item = static_cast<ObTopkItem *>(used_list_.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), KPC(topk_item));
      } else {
        topk_fre_items_.at(i) = *topk_item;
      }
    } else {
      ObTopkDatumItem *datum_item = NULL;
      if (OB_ISNULL(datum_item = static_cast<ObTopkDatumItem *>(used_list_.at(i))) ||
          OB_ISNULL(datum_item->datum_) ||
          OB_ISNULL(obj_meta)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(datum_item), K(obj_meta));
      } else if (OB_FAIL(datum_item->datum_->to_obj(topk_fre_items_.at(i).col_obj_, *obj_meta))) {
        LOG_WARN("failed to obj", K(ret));
      } else {
        topk_fre_items_.at(i).fre_times_ = datum_item->fre_times_;
        topk_fre_items_.at(i).delta_ = datum_item->delta_;
      }
    }
  }
  if (OB_SUCC(ret) && topk_fre_items_.count() > 0) {
    if (OB_FAIL(sort_topk_fre_items(topk_fre_items_))) {
      LOG_WARN("failed to sort items", K(ret));
    } else {
      while (topk_fre_items_.count() > item_size_) {
        topk_fre_items_.pop_back();
      }
      LOG_TRACE("Succeed to adjust topk fre items", K(topk_fre_items_), K(N_), K(by_pass_), K(disuse_cnt_));
    }
  } else {
    LOG_TRACE("Succeed to adjust topk fre items", K(topk_fre_items_), K(N_), K(by_pass_), K(disuse_cnt_));
  }
  return ret;
}

int ObTopKFrequencyHistograms::sort_topk_fre_items(ObIArray<ObTopkItem> &items)
{
  int ret = OB_SUCCESS;
  Compare cmp;
  lib::ob_sort(&items.at(0),
            &items.at(0)+ items.count(),
            CopyableComparer(cmp));
  if (OB_FAIL(cmp.ret_)) {
    ret = cmp.ret_;
    LOG_WARN("failed to sort frequency item", K(ret));
  }
  return ret;
}

void ObTopKFrequencyHistograms::reset_memory_usage()
{
  used_list_.reset();
  free_list_.reset();
  topk_map_.destroy();
  topk_buf_.reset();
  obj_buf1_.reset();
  obj_buf2_.reset();
}

int ObTopKFrequencyHistograms::merge_distribute_top_k_fre_items(const ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObTopKFrequencyHistograms topk_hist;
  if (obj.is_null() || by_pass_) {
    //do nothing
  } else if (OB_UNLIKELY(!obj.is_lob())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(obj), K(ret));
  } else if (OB_UNLIKELY(window_size_ <= 0 ||
                         item_size_ <= 0 ||
                         get_max_reserved_item_size() >= window_size_ ||
                         !is_need_merge_topk_hist())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(window_size_), K(item_size_), K(!is_need_merge_topk_hist()), K(ret));
  } else if (OB_FAIL(topk_hist.read_result(obj))) {
    LOG_WARN("failed to read result from lob data", K(ret));
  } else if (topk_hist.is_by_pass()) {
    N_ += topk_hist.get_total_fre_item();
    disuse_cnt_ += topk_hist.get_total_disuse_fre_item();
    if (is_satisfied_by_pass()) {
      by_pass_ = true;
      reset_memory_usage();
    }
  } else {
    if (OB_UNLIKELY(!topk_map_.created())) {
      int64_t hash_map_size = window_size_ + get_max_reserved_item_size();
      if (OB_FAIL(topk_map_.create(hash_map_size, "TopkMap", "TopkNode", MTL_ID()))) {
        LOG_WARN("failed to create hash map", K(ret));
      } else {
        set_the_obj_memory_use_limit();
      }
    }
    const ObIArray<ObTopkItem> &topk_fre_items = topk_hist.get_buckets();
    for (int64_t i = 0; OB_SUCC(ret) && i < topk_fre_items.count(); ++i) {
      const ObTopkItem &cur_item = topk_fre_items.at(i);
      ObTopkBaseItem *item = NULL;
      uint64_t obj_hash = 0;
      uint64_t seed = 0;
      if (OB_FAIL(topk_fre_items.at(i).col_obj_.hash_murmur(obj_hash, seed))) {
        LOG_WARN("fail to do hash", K(ret));
      } else if (NULL != (item = get_entry(obj_hash))) {
        item->fre_times_ += cur_item.fre_times_;
        item->delta_ += cur_item.delta_;
      } else if (OB_FAIL(add_entry(obj_hash,
                                   cur_item.col_obj_,
                                   cur_item.fre_times_,
                                   cur_item.delta_))) {
        LOG_WARN("failed to add entry", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t bucket_num_old = bucket_num_;
      N_ += topk_hist.get_total_fre_item();
      disuse_cnt_ += topk_hist.get_total_disuse_fre_item();
      bucket_num_ = N_ / window_size_;
      if (bucket_num_ > bucket_num_old) {
        if (OB_FAIL(shrink_topk_items())) {
          LOG_WARN("failed to try shrinking topk fre item", K(ret));
        }
      }
    }
    LOG_TRACE("succeed merge_distribute_top_k_fre_items", K(used_list_));
  }
  return ret;
}

int ObTopKFrequencyHistograms::add_top_k_frequency_item(uint64_t datum_hash, const ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(window_size_ <= 0 ||
                  item_size_ <= 0 ||
                  get_max_reserved_item_size() >= window_size_ ||
                  is_need_merge_topk_hist())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(window_size_), K(item_size_), K(is_need_merge_topk_hist()), K(ret));
  } else if (!datum.is_null()) {
    if (OB_UNLIKELY(!topk_map_.created())) {
      int64_t hash_map_size = window_size_ + get_max_reserved_item_size();
      if (OB_FAIL(topk_map_.create(hash_map_size, "TopkMap", "TopkNode", MTL_ID()))) {
        LOG_WARN("failed to create hash map", K(ret));
      } else {
        set_the_obj_memory_use_limit();
      }
    }
    if (OB_SUCC(ret)) {
      ++ N_;
      if (!by_pass_) {
        ObTopkBaseItem *item = NULL;
        if (NULL != (item = get_entry(datum_hash))) {
          item->fre_times_ ++;
        } else if (OB_FAIL(add_entry(datum_hash, datum, 1L, bucket_num_ - 1))) {
          LOG_WARN("failed to add new entry", K(ret));
        }
        if (OB_SUCC(ret) && N_ % window_size_ == 0) {
          if (OB_FAIL(shrink_topk_items())) {
            LOG_WARN("failed to try shrinking topk fre item", K(ret));
          }
        }
      } else {
        ++ disuse_cnt_;
      }
    }
  }
  return ret;
}

int ObTopKFrequencyHistograms::add_top_k_frequency_item(const ObObj &obj, int64_t repeat_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(window_size_ <= 0 ||
                  item_size_ <= 0 ||
                  get_max_reserved_item_size() >= window_size_ ||
                  !is_need_merge_topk_hist())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(window_size_), K(item_size_), K(!is_need_merge_topk_hist()), K(ret));
  } else if (!obj.is_null()) {
    if (OB_UNLIKELY(!topk_map_.created())) {
      int64_t hash_map_size = window_size_ + get_max_reserved_item_size();
      if (OB_FAIL(topk_map_.create(hash_map_size, "TopkMap", "TopkNode", MTL_ID()))) {
        LOG_WARN("failed to create hash map", K(ret));
      } else {
        set_the_obj_memory_use_limit();
      }
    }
    if (OB_SUCC(ret)) {
      if (!by_pass_) {
        ObTopkBaseItem *item = NULL;
        uint64_t obj_hash = 0;
        uint64_t seed = 0;
        if (OB_FAIL(obj.hash_murmur(obj_hash, seed))) {
          LOG_WARN("fail to do hash", K(ret));
        } else if (NULL != (item = get_entry(obj_hash))) {
          item->fre_times_ += repeat_cnt;
        } else if (OB_FAIL(add_entry(obj_hash, obj, repeat_cnt, bucket_num_ - 1))) {
          LOG_WARN("failed to add new entry", K(ret));
        }
        if (OB_SUCC(ret)) {
          int64_t bucket_num_old = bucket_num_;
          N_ = N_ + repeat_cnt;
          bucket_num_ = N_ > window_size_ ? N_ / window_size_ : bucket_num_;
          if (bucket_num_ > bucket_num_old) {
            if (OB_FAIL(shrink_topk_items())) {
              LOG_WARN("failed to try shrinking topk fre item", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

ObTopkBaseItem* ObTopKFrequencyHistograms::get_entry(const uint64_t hash_val)
{
  ObTopkBaseItem *ret = NULL;
  const uint64_t* ptr = NULL;
  if (NULL != (ptr = topk_map_.get(hash_val))) {
    ret = reinterpret_cast<ObTopkBaseItem*>(*ptr);
  }
  return ret;
}

int ObTopKFrequencyHistograms::add_entry(const uint64_t &datum_hash,
                                         const ObDatum &datum,
                                         const int64_t fre_time,
                                         const int64_t delta)
{
  int ret = OB_SUCCESS;
  ObTopkDatumItem *item = NULL;
  if (OB_ISNULL(item = alloc_topk_datum_item()) ||
      OB_ISNULL(item->datum_ = OB_NEWx(ObDatum, (&get_allocator())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    item->fre_times_ = fre_time;
    item->delta_ = delta;
    item->datum_hash_ = datum_hash;
    if (OB_FAIL(item->datum_->deep_copy(datum, get_allocator()))) {
      LOG_WARN("failed to deep copy datum");
    } else if (OB_FAIL(topk_map_.set_refactored(datum_hash, reinterpret_cast<uint64_t>(item)))) {
      LOG_WARN("failed to add item", K(ret));
    } else if (OB_FAIL(used_list_.push_back(item))) {
      LOG_WARN("failed to push back item", K(ret));
    }
  }
  return ret;
}

int ObTopKFrequencyHistograms::add_entry(const uint64_t &obj_hash,
                                         const ObObj &obj,
                                         const int64_t fre_time,
                                         const int64_t delta)
{
  int ret = OB_SUCCESS;
  ObTopkItem *item = NULL;
  if (OB_ISNULL(item = alloc_topk_item())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to get topk item", K(ret));
  } else {
    item->fre_times_ = fre_time;
    item->delta_ = delta;
    item->obj_hash_ = obj_hash;
    if (OB_FAIL(ob_write_obj(get_allocator(), obj, item->col_obj_))) {
      LOG_WARN("failed to write obj", K(ret));
    } else if (OB_FAIL(topk_map_.set_refactored(obj_hash, reinterpret_cast<uint64_t>(item)))) {
      LOG_WARN("failed to add item", K(ret));
    } else if (OB_FAIL(used_list_.push_back(item))) {
      LOG_WARN("failed to push back item", K(ret));
    }
  }
  return ret;
}

ObTopkDatumItem* ObTopKFrequencyHistograms::alloc_topk_datum_item()
{
  ObTopkDatumItem *item = NULL;
  if (free_list_.empty()) {
    void *ptr = topk_buf_.alloc(sizeof(ObTopkDatumItem));
    if (NULL != ptr) {
      item = new (ptr) ObTopkDatumItem();
    }
  } else {
    item = static_cast<ObTopkDatumItem*>(free_list_.at(free_list_.count() - 1));
    free_list_.pop_back();
  }
  return item;
}

ObTopkItem* ObTopKFrequencyHistograms::alloc_topk_item()
{
  ObTopkItem *item = NULL;
  if (free_list_.empty()) {
    void *ptr = topk_buf_.alloc(sizeof(ObTopkItem));
    if (NULL != ptr) {
      item = new (ptr) ObTopkItem();
    }
  } else {
    item = static_cast<ObTopkItem*>(free_list_.at(free_list_.count() - 1));
    free_list_.pop_back();
  }
  return item;
}

int ObTopKFrequencyHistograms::remove_entry(ObTopkBaseItem *item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item is null", K(ret));
  } else if (is_need_merge_topk_hist() &&
             OB_FAIL(topk_map_.erase_refactored(static_cast<ObTopkItem*>(item)->obj_hash_))) {
    LOG_WARN("failed to erase item", K(ret));
  } else if (!is_need_merge_topk_hist() &&
             OB_FAIL(topk_map_.erase_refactored(static_cast<ObTopkDatumItem*>(item)->datum_hash_))) {
    LOG_WARN("failed to erase item", K(ret));
  } else if (OB_FAIL(free_list_.push_back(item))) {
    LOG_WARN("faild to push back item", K(ret));
  }
  return ret;
}

/**
 * @brief ObTopKFrequencyHistograms::shrink_topk_fre_items
 *     shrinking phase; remove infrequent item
 * @return
 */
int ObTopKFrequencyHistograms::shrink_topk_items()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t rm_cnt = 0;
  int64_t max_rm_cnt = used_list_.count() - get_max_reserved_item_size();
  for (int64_t i  = 0; OB_SUCC(ret) && i < used_list_.count(); ++i) {
    ObTopkBaseItem *item = NULL;
    if (OB_ISNULL(item = used_list_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(i));
    } else if (item->fre_times_ + item->delta_ > bucket_num_ ||
               rm_cnt >= max_rm_cnt) {
      used_list_.at(pos++) = item;
    } else if (OB_FAIL(remove_entry(item))) {
      LOG_WARN("failed to remove item", K(ret));
    } else {
      ++ rm_cnt;
      disuse_cnt_ += item->fre_times_;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_satisfied_by_pass()) {
      by_pass_ = true;
      reset_memory_usage();
    } else {
      while (used_list_.count() > pos) {
        used_list_.pop_back();
      }
      if (OB_UNLIKELY(obj_memory_limit_ < MIN_OBJ_MEMORY_LIMIT ||
                      obj_memory_limit_ > MAX_OBJ_MEMORY_LIMIT)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(obj_memory_limit_));
      } else if (OB_UNLIKELY(get_allocator().used() > obj_memory_limit_)) {
        if (OB_FAIL(shrink_memory_usage())) {
          LOG_WARN("failed to shrink memory usage", K(ret));
        }
      }
      bucket_num_ = N_ / window_size_ + 1;//next window size should add 1.
    }
  }
  return ret;
}

bool ObTopKFrequencyHistograms::is_satisfied_by_pass() const
{
  bool is_satisfied = false;
  if (max_disuse_cnt_ > 0) {
    is_satisfied = disuse_cnt_ >= max_disuse_cnt_;
  } else if (N_ % (window_size_ * 100) == 0) {
    if (1.0 * disuse_cnt_ / N_ > get_current_min_topk_ratio()) {
      is_satisfied = true;
      LOG_INFO("topk hist is statisfied by pass", K(disuse_cnt_), K(N_), K(get_current_min_topk_ratio()));
    }
  }
  return is_satisfied;
}

/* as following rule to set min topk ratio, Assume the total min topk ratio is: r
 * if N_ < window_size_ * 1000:
 *    min_ratio at least r/2;
 * if N_ < window_size_ * 10000:
 *    min_ratio at least r/2 + r/4
 * if N_ between window_size_ * 10000 and window_size_ * 100000:
 *    increase per window_size_ * 10000, the min_ratio increase: r/2 + r/4+...r/(2^x)
 * if N_ more than window_size_ * 100000, at least 100000000 rows:
 *    min_ratio set the r.
 *
*/
double ObTopKFrequencyHistograms::get_current_min_topk_ratio() const
{
  double min_topk_ratio = 1.0 - 1.0 / item_size_;
  if (N_ < window_size_ * 1000) {
    min_topk_ratio = min_topk_ratio / 2;
  } else if (N_ < window_size_ * 10000) {
    min_topk_ratio = min_topk_ratio / 2 + min_topk_ratio / 4;
  } else if (N_ >= window_size_ * 10000 && N_ < window_size_ * 100000) {
    int64_t n = N_ / (window_size_ * 10000) + 3;
    int64_t power = 2;
    double new_min_topk_ratio = 0.0;
    for (int i = 1; i < n; i++) {
      power *= i;
      new_min_topk_ratio += min_topk_ratio / power;
    }
    min_topk_ratio = new_min_topk_ratio;
  }
  LOG_TRACE("get_current_min_topk_ratio", K(min_topk_ratio), K(item_size_), K(N_));
  return min_topk_ratio;
}

//allocte object total memory used restict[1MB ~ 10MB]
void ObTopKFrequencyHistograms::set_the_obj_memory_use_limit()
{
  //default is current tenant limit's 1/1000
  obj_memory_limit_ = lib::get_tenant_memory_limit(MTL_ID()) / 1000;
  //then see the sql work arena limit, set the limit's 1/100
  if (lib::ObMallocAllocator::get_instance() != NULL) {
    auto ta = lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(MTL_ID(), common::ObCtxIds::WORK_AREA);
    obj_memory_limit_ = std::min(obj_memory_limit_, ta->get_limit() / 100);
  }
  obj_memory_limit_ = std::max(obj_memory_limit_, MIN_OBJ_MEMORY_LIMIT);
  obj_memory_limit_ = std::min(obj_memory_limit_, MAX_OBJ_MEMORY_LIMIT);
  LOG_TRACE("set the memory use limit", K(obj_memory_limit_));
}

/**
 * @brief ObTopKFrequencyHistograms::shrink_memory_usage
 * we use two buffer in turn.
 * the memory we used would be about: the size of object * window_size.
 * @return
 */
int ObTopKFrequencyHistograms::shrink_memory_usage()
{
  int ret = OB_SUCCESS;
  ObIAllocator &next_allocator = get_next_allocator();
  ObIAllocator &curr_allocator = get_allocator();
  topk_map_.reuse();
  LOG_TRACE("begine to shrink memory usage", K(obj_memory_limit_), K(curr_allocator.used()));
  for (int64_t i = 0; OB_SUCC(ret) && i < used_list_.count(); ++i) {
    if (OB_ISNULL(used_list_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("topk item is null", K(ret));
    } else if (is_need_merge_topk_hist()) {
      ObTopkItem *topk_item = NULL;
      if (OB_ISNULL(topk_item = static_cast<ObTopkItem *>(used_list_.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), KPC(topk_item));
      } else if (OB_FAIL(ob_write_obj(next_allocator,
                                      topk_item->col_obj_,
                                      topk_item->col_obj_))) {
        LOG_WARN("failed to write obj", K(ret));
      } else if (OB_FAIL(topk_map_.set_refactored(topk_item->obj_hash_,
                                                  reinterpret_cast<uint64_t>(topk_item)))) {
        LOG_WARN("failed to add item", K(ret));
      }
    } else {
      ObTopkDatumItem *datum_item = NULL;
      ObDatum *tmp_datum = NULL;
      if (OB_ISNULL(datum_item = static_cast<ObTopkDatumItem *>(used_list_.at(i))) ||
          OB_ISNULL(tmp_datum = datum_item->datum_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), KPC(datum_item));
      } else if (OB_ISNULL(datum_item->datum_ = OB_NEWx(ObDatum, (&next_allocator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else if (OB_FAIL(datum_item->datum_->deep_copy(*tmp_datum, next_allocator))) {
        LOG_WARN("failed to write obj", K(ret));
      } else if (OB_FAIL(topk_map_.set_refactored(datum_item->datum_hash_,
                                                  reinterpret_cast<uint64_t>(datum_item)))) {
        LOG_WARN("failed to add item", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    curr_allocator.reset();
    swap_allocator();
  }
  return ret;
}

int ObTopKFrequencyHistograms::read_result(const ObObj &result_obj)
{
  int ret = OB_SUCCESS;
  ObString result_str;
  int64_t pos = 0;
  if (result_obj.is_null()) {
    // do nothing
  } else if (OB_UNLIKELY(!result_obj.is_lob())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob is expected", K(ret));
  } else if (OB_FAIL(result_obj.get_string(result_str))) {
    LOG_WARN("failed to get string", K(ret));
  } else if (OB_FAIL(deserialize(result_str.ptr(), result_str.length(), pos))) {
    LOG_WARN("failed to deserialize histograms from buffer", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTopKFrequencyHistograms)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(N_);
  int64_t items_count = topk_fre_items_.count();
  OB_UNIS_ENCODE(items_count);
  for (int64_t i = 0; OB_SUCC(ret) && i < items_count; ++i) {
    const ObTopkItem &top_k_fre_item = topk_fre_items_.at(i);
    OB_UNIS_ENCODE(top_k_fre_item.col_obj_);
    OB_UNIS_ENCODE(top_k_fre_item.fre_times_);
    OB_UNIS_ENCODE(top_k_fre_item.delta_);
  }
  OB_UNIS_ENCODE(by_pass_);
  OB_UNIS_ENCODE(disuse_cnt_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTopKFrequencyHistograms)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(N_);
  int64_t items_count = topk_fre_items_.count();
  OB_UNIS_ADD_LEN(items_count);
  for (int64_t i = 0; i < topk_fre_items_.count(); ++i) {
    const ObTopkItem &top_k_fre_item = topk_fre_items_.at(i);
    OB_UNIS_ADD_LEN(top_k_fre_item.col_obj_);
    OB_UNIS_ADD_LEN(top_k_fre_item.fre_times_);
    OB_UNIS_ADD_LEN(top_k_fre_item.delta_);
  }
  OB_UNIS_ADD_LEN(by_pass_);
  OB_UNIS_ADD_LEN(disuse_cnt_);
  return len;
}

OB_DEF_DESERIALIZE(ObTopKFrequencyHistograms)
{
  int ret = OB_SUCCESS;
  int64_t items_count = 0;
  OB_UNIS_DECODE(N_);
  OB_UNIS_DECODE(items_count);
  topk_fre_items_.set_allocator(&topk_buf_);
  if (OB_FAIL(topk_fre_items_.init(items_count))) {
    LOG_WARN("failed to init fixed array", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < items_count; ++i) {
    ObTopkItem top_k_fre_item;
    OB_UNIS_DECODE(top_k_fre_item.col_obj_);
    OB_UNIS_DECODE(top_k_fre_item.fre_times_);
    OB_UNIS_DECODE(top_k_fre_item.delta_);
    if (OB_FAIL(topk_fre_items_.push_back(top_k_fre_item))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  OB_UNIS_DECODE(by_pass_);
  OB_UNIS_DECODE(disuse_cnt_);
  return ret;
}

ObTopkHistEstimator::ObTopkHistEstimator(ObExecContext &ctx, ObIAllocator &allocator)
  : ObBasicStatsEstimator(ctx, allocator)
{}

int ObTopkHistEstimator::estimate(const ObOptStatGatherParam &param,
                                  ObOptStat &opt_stat)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("ObTopkHistEst", OB_MALLOC_NORMAL_BLOCK_SIZE, param.tenant_id_);
  ObSqlString raw_sql;
  int64_t duration_time = -1;
  ObSEArray<ObOptStat, 1> tmp_opt_stats;
  if (OB_FAIL(add_topk_hist_stat_items(param.column_params_, opt_stat))) {
    LOG_WARN("failed to add topk hist stat items", K(ret));
  } else if (get_item_size() <= 0) {
    //no need topk histogram item.
  } else if (OB_FAIL(fill_hints(allocator, param.tab_name_, param.gather_vectorize_, false))) {
    LOG_WARN("failed to fill hints", K(ret));
  } else if (OB_FAIL(add_from_table(param.db_name_, param.tab_name_))) {
    LOG_WARN("failed to add from table", K(ret));
  } else if (OB_UNLIKELY(param.partition_infos_.count() > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param));
  } else if (!param.partition_infos_.empty() &&
             OB_FAIL(fill_partition_info(allocator, param.partition_infos_.at(0).part_name_))) {
    LOG_WARN("failed to add partition info", K(ret));
  } else if (OB_FAIL(fill_parallel_info(allocator, param.degree_))) {
    LOG_WARN("failed to add query sql parallel info", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::get_valid_duration_time(param.gather_start_time_,
                                                               param.max_duration_time_,
                                                               duration_time))) {
    LOG_WARN("failed to get valid duration time", K(ret));
  } else if (OB_FAIL(fill_query_timeout_info(allocator, duration_time))) {
    LOG_WARN("failed to fill query timeout info", K(ret));
  } else if (OB_FAIL(fill_sample_info(allocator, param.sample_info_))) {
    LOG_WARN("failed to fill sample info", K(ret));
  } else if (OB_FAIL(fill_specify_scn_info(allocator, param.sepcify_scn_))) {
    LOG_WARN("failed to fill specify scn info", K(ret));
  } else if (OB_FAIL(pack(raw_sql))) {
    LOG_WARN("failed to pack raw sql", K(ret));
  } else if (OB_FAIL(tmp_opt_stats.push_back(opt_stat))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(do_estimate(param.tenant_id_, raw_sql.string(), false,
                                 opt_stat, tmp_opt_stats))) {
    LOG_WARN("failed to evaluate basic stats", K(ret));
  } else {
    LOG_TRACE("succeed to gather topk histogram", K(opt_stat.column_stats_));
  }
  return ret;
}

int ObTopkHistEstimator::add_topk_hist_stat_items(const ObIArray<ObColumnStatParam> &column_params,
                                                  ObOptStat &opt_stat)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(opt_stat.table_stat_) ||
      OB_UNLIKELY(opt_stat.column_stats_.count() != column_params.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(opt_stat), K(column_params));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_params.count(); ++i) {
      const ObColumnStatParam *col_param = &column_params.at(i);
      if (OB_ISNULL(opt_stat.column_stats_.at(i)) ||
          OB_UNLIKELY(col_param->column_id_ != opt_stat.column_stats_.at(i)->get_column_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), KPC(opt_stat.column_stats_.at(i)), KPC(col_param));
      } else if (opt_stat.column_stats_.at(i)->get_histogram().get_type() != ObHistType::TOP_FREQUENCY) {
        //do nothing
      } else {
        int64_t max_disuse_cnt = std::ceil(opt_stat.column_stats_.at(i)->get_num_not_null() * 1.0 / col_param->bucket_num_);
        if (OB_FAIL(add_stat_item(ObStatTopKHist(col_param,
                                                 opt_stat.table_stat_,
                                                 opt_stat.column_stats_.at(i),
                                                 max_disuse_cnt)))) {
          LOG_WARN("failed to add statistic item", K(ret));
        } else {/*do noting*/}
      }
    }
  }
  return ret;
}

}
}
