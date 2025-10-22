/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_SPARSE_BMW_ITER_H_
#define OB_SPARSE_BMW_ITER_H_

#include "lib/container/ob_heap.h"
#include "ob_sparse_daat_iter.h"
#include "ob_sparse_retrieval_util.h"

namespace oceanbase
{
namespace storage
{
class ObSRBMWIterImpl : public ObSRDaaTIterImpl
{
public:
  ObSRBMWIterImpl();
  virtual ~ObSRBMWIterImpl() {}

  virtual void reuse(const bool switch_tablet = false) override;
  void reset();
  int init(
      ObSparseRetrievalMergeParam &iter_param,
      ObIArray<ObISRDaaTDimIter *> &dim_iters,
      ObIAllocator &iter_allocator,
      ObSRDaaTRelevanceCollector &relevance_collector);
  virtual int get_next_rows(const int64_t capacity, int64_t &count) override;
protected:
  virtual int process_collected_row(const ObDatum &id_datum, const double relevance) override;
  virtual int init_before_wand_process() { return OB_SUCCESS; }

  int top_k_search();
  int build_top_k_heap();
  int next_pivot(int64_t &pivot_iter_idx);
  int next_pivot_range(int64_t &skip_range_cnt);
  int evaluate_pivot(const int64_t pivot_iter_idx);
  int evaluate_pivot_range(const int64_t pivot_iter_idx, bool &is_candidate);
  double get_top_k_threshold()
  {
    return top_k_heap_.empty() ? 0.0 : top_k_heap_.top().relevance_;
  }

private:
  ObISRDimBlockMaxIter *get_iter(const int64_t iter_idx) const
  {
    return static_cast<ObISRDimBlockMaxIter *>(dim_iters_->at(iter_idx));
  }
  int fill_merge_heap_with_shallow_dims(const ObDatum *last_range_border_id, const bool inclusive);
  int try_generate_next_range_from_merge_heap(
      bool &is_candidate_range,
      const ObDatum *&min_domain_id_with_pivot,
      const ObDatum *&max_domain_id_without_pivot);
  int project_rows_from_top_k_heap(const int64_t capacity, int64_t &count);
  int unify_dim_iters_for_next_round();
  int advance_dim_iters_for_next_round(const ObDatum &target_id, const bool iter_end_available);
  void set_next_round_iter_end(const int64_t idx) { next_round_iter_idxes_[idx] = -1; }
  bool is_next_round_iter_end(const int64_t idx) const { return next_round_iter_idxes_[idx] == -1; }

protected:
  enum BMWStatus{
    FIND_NEXT_PIVOT,
    EVALUATE_PIVOT_RANGE,
    EVALUATE_PIVOT,
    FIND_NEXT_PIVOT_RANGE,
    FINISHED,
    MAX_STATUS,
  };
  struct TopKItem {
    TopKItem() : relevance_(0.0), cache_idx_(-1) {}
    TopKItem(const double &relevance, const int64_t &cache_idx) : relevance_(relevance), cache_idx_(cache_idx) {}
    ~TopKItem() = default;
    TO_STRING_KV(K_(relevance), K_(cache_idx));
    double relevance_;
    int64_t cache_idx_;
  };

  struct TopKItemCmp {
    bool operator()(const TopKItem &a, const TopKItem &b) const {
      return a.relevance_ > b.relevance_;
    }
    int get_error_code() { return common::OB_SUCCESS; }
  };
  typedef common::ObBinaryHeap<TopKItem, TopKItemCmp> TopKHeap;

  DISALLOW_COPY_AND_ASSIGN(ObSRBMWIterImpl);
  ObArenaAllocator allocator_;
  TopKItemCmp less_score_cmp_;
  // Maybe domain id not need to be cached within the topk heap since there would be top-n sort with datum store above
  TopKHeap top_k_heap_;
  int64_t top_k_count_;
  ObDomainIdCmp domain_id_cmp_;
  ObFixedArray<ObDocIdExt, ObIAllocator> id_cache_;
  BMWStatus status_;
};


} // namespace storage
} // namespace oceanbase
#endif
