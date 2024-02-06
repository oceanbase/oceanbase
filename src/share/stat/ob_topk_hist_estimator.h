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
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace common
{

struct ObTopkBaseItem
{
  ObTopkBaseItem()
    : fre_times_(0),
      delta_(0)
  {
  }

  TO_STRING_KV(K_(fre_times),
               K_(delta));
  int64_t fre_times_;
  int64_t delta_;
};

struct ObTopkItem : public ObTopkBaseItem
{
  ObTopkItem()
    : ObTopkBaseItem(),
      obj_hash_(0),
      col_obj_()
  {
  }

  TO_STRING_KV(K_(fre_times),
               K_(delta),
               K_(obj_hash),
               K_(col_obj));
  uint64_t obj_hash_;
  common::ObObj col_obj_;
};

struct ObTopkDatumItem : public ObTopkBaseItem
{
  ObTopkDatumItem()
    : ObTopkBaseItem(),
      datum_hash_(0),
      datum_(NULL)
  {
  }

  TO_STRING_KV(K_(fre_times),
               K_(delta),
               K_(datum_hash),
               KPC_(datum));
  uint64_t datum_hash_;
  ObDatum *datum_;
};

typedef common::hash::ObHashMap<uint64_t, uint64_t, common::hash::NoPthreadDefendMode> ObTopkMap;

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
      topk_buf_("OptTopkHistbuf"),
      use_first_alloc_(true),
      obj_buf1_("OptTopObjbuf1"),
      obj_buf2_("OptTopObjbuf2"),
      by_pass_(false),
      disuse_cnt_(0),
      obj_memory_limit_(0)
    {
      topk_buf_.set_tenant_id(MTL_ID());
      obj_buf1_.set_tenant_id(MTL_ID());
      obj_buf2_.set_tenant_id(MTL_ID());
      used_list_.set_attr(lib::ObMemAttr(MTL_ID(), "ObTopkUsedlist"));
      free_list_.set_attr(lib::ObMemAttr(MTL_ID(), "ObTopkfreelist"));
    }

    int read_result(const ObObj &result_obj);
    int create_topk_fre_items(const ObObjMeta *obj_meta = NULL);
    int sort_topk_fre_items(ObIArray<ObTopkItem> &items);
    int shrink_topk_items();
    int add_top_k_frequency_item(const ObObj &obj);
    int add_top_k_frequency_item(uint64_t datum_hash, const ObDatum &datum);
    int merge_distribute_top_k_fre_items(const ObObj &obj);
    void set_window_size(int64_t window_size) { window_size_ = window_size; }
    void set_item_size(int64_t item_size) { item_size_ = item_size; }
    void set_is_topk_hist_need_des_row(bool is_true) { is_topk_hist_need_des_row_ = is_true; }
    bool is_need_merge_topk_hist() const { return is_topk_hist_need_des_row_; }
    const ObIArray<ObTopkItem> &get_buckets() const { return topk_fre_items_; }
    bool has_bucket() const { return !used_list_.empty(); }
    int64_t get_total_fre_item() const { return N_; }
    ObTopkBaseItem *get_entry(const uint64_t hash_val);
    int add_entry(const uint64_t &datum_hash, const ObDatum &datum, const int64_t fre_time, const int64_t delta);
    int add_entry(const uint64_t &obj_hash, const ObObj &obj, const int64_t fre_time, const int64_t delta);
    int remove_entry(ObTopkBaseItem *item);
    int shrink_memory_usage();
    void reset_memory_usage();
    bool is_by_pass() const { return by_pass_; }
    bool is_satisfied_by_pass() const;
    int64_t get_total_disuse_fre_item() const { return disuse_cnt_; }
    double get_current_min_topk_ratio() const;
    void set_the_obj_memory_use_limit();
    int64_t get_max_reserved_item_size() const { return item_size_ * 1.5; }
    ObTopkItem *alloc_topk_item();
    ObTopkDatumItem *alloc_topk_datum_item();
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
                 K_(topk_fre_items),
                 K_(by_pass),
                 K_(disuse_cnt),
                 K_(obj_memory_limit));
  private:
    const int64_t MIN_OBJ_MEMORY_LIMIT = 1LL * 1024LL * 1024LL; // 1MB
    const int64_t MAX_OBJ_MEMORY_LIMIT = 10LL * 1024LL * 1024LL; // 10MB
    int64_t N_;
    int64_t bucket_num_;
    int64_t window_size_; // in default, the  window size is 1000
    int64_t item_size_;
    bool is_topk_hist_need_des_row_; // for parallel topk histogram compuation
    common::ObFixedArray<ObTopkItem, common::ObIAllocator> topk_fre_items_;
    // the following are used to computation
    common::ObSEArray<ObTopkBaseItem*, 16> used_list_;
    common::ObSEArray<ObTopkBaseItem*, 16> free_list_;
    ObTopkMap topk_map_;
    ObArenaAllocator topk_buf_;
    // obj_buf1_ and obj_buf2_ are used in turn
    // true for obj_buf1_, false for obj_buf2_
    bool use_first_alloc_;
    ObArenaAllocator obj_buf1_;
    ObArenaAllocator obj_buf2_;
    bool by_pass_;
    int64_t disuse_cnt_;
    int64_t obj_memory_limit_;
};

} // end of namespace common
} // end of namespace oceanbase

#endif /*#endif OB_TOPK_HIST_ESTIMATOR_H */
