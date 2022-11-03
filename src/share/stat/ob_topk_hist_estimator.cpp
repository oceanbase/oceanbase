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
int ObTopKFrequencyHistograms::create_topk_fre_items()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < used_list_.count(); ++i) {
    if (OB_ISNULL(used_list_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("used list is null", K(ret));
    } else if (OB_FAIL(topk_fre_items_.push_back(*used_list_.at(i)))) {
      LOG_WARN("failed to push back topk item", K(ret));
    }
  }
  if (OB_SUCC(ret) && topk_fre_items_.count() > 0) {
    if (OB_FAIL(sort_topk_fre_items(topk_fre_items_))) {
      LOG_WARN("failed to sort items", K(ret));
    } else {
      while (topk_fre_items_.count() > item_size_) {
        topk_fre_items_.pop_back();
      }
      LOG_TRACE("Succeed to adjust topk fre items", K(topk_fre_items_));
    }
  } else {
    LOG_TRACE("Succeed to adjust topk fre items", K(topk_fre_items_));
  }
  return ret;
}

int ObTopKFrequencyHistograms::sort_topk_fre_items(ObIArray<ObTopkItem> &items)
{
  int ret = OB_SUCCESS;
  Compare cmp;
  std::sort(&items.at(0),
            &items.at(0)+ items.count(),
            CopyableComparer(cmp));
  if (OB_FAIL(cmp.ret_)) {
    ret = cmp.ret_;
    LOG_WARN("failed to sort frequency item", K(ret));
  }
  return ret;
}

int ObTopKFrequencyHistograms::merge_distribute_top_k_fre_items(const ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObTopKFrequencyHistograms topk_hist;
  //1.calc frequency times
  if (OB_UNLIKELY(!obj.is_lob())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(obj), K(ret));
  } else if (OB_UNLIKELY(window_size_ <= 0 || item_size_ <= 0)) {
    /*do nothing*/
  } else if (OB_FAIL(topk_hist.read_result(obj))) {
    LOG_WARN("failed to read result from lob data", K(ret));
  } else {
    const ObIArray<ObTopkItem> &topk_fre_items = topk_hist.get_buckets();
    for (int64_t i = 0; OB_SUCC(ret) && i < topk_fre_items.count(); ++i) {
      const ObTopkItem &cur_item = topk_fre_items.at(i);
      ObTopkItem *item = NULL;
      if (NULL != (item = get_entry(topk_fre_items.at(i).col_obj_))) {
        item->fre_times_ += cur_item.fre_times_;
        item->delta_ += cur_item.delta_;
      } else if (OB_FAIL(add_entry(cur_item.col_obj_,
                                   cur_item.fre_times_,
                                   cur_item.delta_))) {
        LOG_WARN("failed to add entry", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t bucket_num_old = bucket_num_;
      N_ = N_ + topk_hist.get_total_fre_item();
      bucket_num_ = N_ / window_size_;
      if (bucket_num_ > bucket_num_old) {
        if (OB_FAIL(shrink_topk_items())) {
          LOG_WARN("failed to try shrinking topk fre item");
        }
      }
    }
    LOG_TRACE("succeed merge_distribute_top_k_fre_items", K(used_list_));
  }
  return ret;
}

int ObTopKFrequencyHistograms::add_top_k_frequency_item(const ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(window_size_ <= 0 || item_size_ <= 0)) {
    // TODO jiangxiu.wt, better to directly return error
    // do nothing
  } else if (!obj.is_null()) {
    if (OB_UNLIKELY(!topk_map_.created())) {
      const int64_t hashmap_size = 2 * (window_size_ + item_size_);
      if (OB_FAIL(topk_map_.create(hashmap_size, "TopkMap"))) {
        LOG_WARN("failed to create hash map", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_UNLIKELY(is_topk_hist_need_des_row_)) {
      if (OB_FAIL(merge_distribute_top_k_fre_items(obj))) {
        LOG_WARN("failed to process distribute row", K(ret));
      }
    } else {
      ++ N_;
      ObTopkItem *item = NULL;
      if (NULL != (item = get_entry(obj))) {
        item->fre_times_ ++;
      } else if (OB_FAIL(add_entry(obj, 1L, bucket_num_ - 1))) {
        LOG_WARN("failed to add new entry", K(ret));
      }
      if (OB_SUCC(ret) && N_ % window_size_ == 0) {
        if (OB_FAIL(shrink_topk_items())) {
          LOG_WARN("failed to try shrinking topk fre item");
        }
      }
    }
  }
  return ret;
}

ObTopkItem* ObTopKFrequencyHistograms::get_entry(const ObObj &obj)
{
  ObTopkItem *ret = NULL;
  const uint64_t* ptr = NULL;
  if (NULL != (ptr = topk_map_.get(obj))) {
    ret = reinterpret_cast<ObTopkItem*>(*ptr);
  }
  return ret;
}

int ObTopKFrequencyHistograms::add_entry(const ObObj &obj,
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
    if (OB_FAIL(ob_write_obj(get_allocator(), obj, item->col_obj_))) {
      LOG_WARN("failed to write obj", K(ret));
    } else if (OB_FAIL(topk_map_.set_refactored(item->col_obj_,
                                                reinterpret_cast<uint64_t>(item)))) {
      LOG_WARN("failed to add item", K(ret));
    } else if (OB_FAIL(used_list_.push_back(item))) {
      LOG_WARN("failed to push back item", K(ret));
    } else if (obj.need_deep_copy()) {
      ++ copied_count_;
    }
  }
  return ret;
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
    item = free_list_.at(free_list_.count() - 1);
    free_list_.pop_back();
  }
  return item;
}

int ObTopKFrequencyHistograms::remove_entry(ObTopkItem *item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item is null", K(ret));
  } else if (OB_FAIL(topk_map_.erase_refactored(item->col_obj_))) {
    LOG_WARN("failed to erase item", K(ret), K(item->col_obj_));
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
  int64_t max_rm_cnt = used_list_.count() - item_size_;
  for (int64_t i  = 0; OB_SUCC(ret) && i < used_list_.count(); ++i) {
    ObTopkItem *item = NULL;
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
    }
  }
  while (used_list_.count() > pos) {
    used_list_.pop_back();
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(copied_count_ >= 100 * window_size_)) {
    if (OB_FAIL(shrink_memory_usage())) {
      LOG_WARN("failed to shrink memory usage", K(ret));
    }
  }
  bucket_num_ = N_ / window_size_ + 1;//next window size should add 1.
  return ret;
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
  copied_count_ = 0;
  topk_map_.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < used_list_.count(); ++i) {
    ObTopkItem *item = NULL;
    if (OB_ISNULL(item = used_list_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("topk item is null", K(ret));
    } else if (OB_FAIL(ob_write_obj(next_allocator,
                                    item->col_obj_,
                                    item->col_obj_))) {
      LOG_WARN("failed to write obj", K(ret));
    } else if (OB_FAIL(topk_map_.set_refactored(item->col_obj_,
                                                reinterpret_cast<uint64_t>(item)))) {
      LOG_WARN("failed to add item", K(ret));
    } else if (item->col_obj_.need_deep_copy()) {
      ++copied_count_;
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
  return len;
}

OB_DEF_DESERIALIZE(ObTopKFrequencyHistograms)
{
  int ret = OB_SUCCESS;
  int64_t items_count = 0;
  OB_UNIS_DECODE(N_);
  OB_UNIS_DECODE(items_count);
  for (int64_t i = 0; OB_SUCC(ret) && i < items_count; ++i) {
    ObTopkItem top_k_fre_item;
    OB_UNIS_DECODE(top_k_fre_item.col_obj_);
    OB_UNIS_DECODE(top_k_fre_item.fre_times_);
    OB_UNIS_DECODE(top_k_fre_item.delta_);
    if (OB_FAIL(topk_fre_items_.push_back(top_k_fre_item))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}
}
}
