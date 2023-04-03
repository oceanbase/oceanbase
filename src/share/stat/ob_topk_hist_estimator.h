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

#ifndef OB_TOPK_HIST_ESTIMATOR_H
#define OB_TOPK_HIST_ESTIMATOR_H

#include "share/stat/ob_stat_define.h"

namespace oceanbase
{
namespace common
{

struct ObTopkItem
{
  ObTopkItem()
    : fre_times_(0),
      delta_(0),
      col_obj_()
  {
  }

  TO_STRING_KV(K_(fre_times),
               K_(delta),
               K_(col_obj));
  int64_t fre_times_;
  int64_t delta_;
  common::ObObj col_obj_;
};

typedef common::hash::ObHashMap<ObObj, uint64_t, common::hash::NoPthreadDefendMode> ObTopkMap;

class ObTopKFrequencyHistograms
{
  OB_UNIS_VERSION(1);
  public:
    ObTopKFrequencyHistograms()
    : N_(0),
      bucket_num_(1),
      window_size_(0),
      item_size_(0),
      is_topk_hist_need_des_row_(false),
      topk_fre_items_(),
      used_list_(),
      free_list_(),
      topk_map_(),
      topk_buf_(),
      use_first_alloc_(true),
      obj_buf1_(),
      obj_buf2_(),
      copied_count_()
    {}

    int read_result(const ObObj &result_obj);

    int create_topk_fre_items();
    int sort_topk_fre_items(ObIArray<ObTopkItem> &items);
    int shrink_topk_items();
    int add_top_k_frequency_item(const ObObj &obj);
    int merge_distribute_top_k_fre_items(const ObObj &obj);
    void set_window_size(int64_t window_size) { window_size_ = window_size; }
    void set_item_size(int64_t item_size) { item_size_ = item_size; }
    void set_is_topk_hist_need_des_row(bool is_true) { is_topk_hist_need_des_row_ = is_true; }
    const ObIArray<ObTopkItem> &get_buckets() const { return topk_fre_items_; }
    bool has_bucket() const { return !used_list_.empty(); }
    int64_t get_total_fre_item() const { return N_; }

    ObTopkItem *get_entry(const ObObj &obj);
    int add_entry(const ObObj &obj, const int64_t fre, const int64_t delta);
    int remove_entry(ObTopkItem *item);

    int shrink_memory_usage();

    ObTopkItem *alloc_topk_item();

    ObIAllocator &get_allocator()
    {
      if (use_first_alloc_) {
        return obj_buf1_;
      } else {
        return obj_buf2_;
      }
    }

    ObIAllocator &get_next_allocator()
    {
      if (use_first_alloc_) {
        return obj_buf2_;
      } else {
        return obj_buf1_;
      }
    }

    void swap_allocator()
    {
      use_first_alloc_ = !use_first_alloc_;
    }

    TO_STRING_KV(K_(N),
                 K_(bucket_num),
                 K_(window_size),
                 K_(item_size),
                 K_(is_topk_hist_need_des_row),
                 K_(topk_fre_items));
  private:
    int64_t N_;
    int64_t bucket_num_;
    int64_t window_size_; // in default, the  window size is 1000
    int64_t item_size_;
    bool is_topk_hist_need_des_row_; // for parallel topk histogram compuation
    common::ObSEArray<ObTopkItem, 16, common::ModulePageAllocator, true> topk_fre_items_;

    // the following are used to computation
    common::ObSEArray<ObTopkItem*, 16, common::ModulePageAllocator, true> used_list_;
    common::ObSEArray<ObTopkItem*, 16, common::ModulePageAllocator, true> free_list_;
    ObTopkMap topk_map_;
    ObArenaAllocator topk_buf_;

    // obj_buf1_ and obj_buf2_ are used in turn
    // true for obj_buf1_, false for obj_buf2_
    bool use_first_alloc_;
    ObArenaAllocator obj_buf1_;
    ObArenaAllocator obj_buf2_;

    int64_t copied_count_;
};

} // end of namespace common
} // end of namespace oceanbase

#endif /*#endif OB_TOPK_HIST_ESTIMATOR_H */
