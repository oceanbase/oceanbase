/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#ifndef OB_SPARSE_BMM_ITER_H_
#define OB_SPARSE_BMM_ITER_H_

#include "ob_sparse_bmw_iter.h"

namespace oceanbase
{
namespace storage
{

// Block Max MaxScore top-k pruning implementation
// merge_heap is used as multi-way merge heap for inner production calculation
// Here we reuse merge_heap as essential dims merge heap for MaxScore pruning
class ObSRBMMIterImpl : public ObSRBlockMaxTopKIterImpl
{
public:
  ObSRBMMIterImpl();
  virtual ~ObSRBMMIterImpl() {}
  virtual void reuse(const bool switch_tablet = false) override;
  void reset();
  int init(
      ObSparseRetrievalMergeParam &iter_param,
      ObIArray<ObISRDaaTDimIter *> &dim_iters,
      ObIAllocator &iter_allocator,
      ObSRDaaTRelevanceCollector &relevance_collector);
protected:
  virtual int top_k_search() override;
private:
  static constexpr int64_t DEFAULT_BLOCK_MERGE_BUCKET_SIZE = 256;
  double get_essential_dim_threshold() const { return get_top_k_threshold() - non_essential_dim_max_score_; }
  int sort_and_classify_dims();
  int try_update_essential_dims();
  bool can_use_block_merge() const;
  int init_block_merge_buffers();
  void reuse_block_merge_buffers();
  int get_uint_doc_id(const ObDatum &datum, uint64_t &doc_id) const;
  int next_pivot(ObDocIdExt &pivot_id);
  int decide_block_merge_upper_bound(const ObMaxScoreTuple &max_score_tuple, bool &found_range_upper_bound);
  int evaluate_pivot_range(const ObDatum &pivot_id, double &non_essential_block_max_score, bool &is_candidate);
  int evaluate_essential_pivot(
      const ObDatum &pivot_id,
      const double non_essential_block_max_score,
      double &essential_score,
      bool &is_candidate,
      bool &is_valid_pivot);
  int evaluate_pivot(const ObDatum &pivot_id, const double &essential_score);
  int forward_next_round_iters();
  int block_merge_pivot_range(const ObDatum &range_min_id, const double non_essential_block_max_score);
  int block_merge_essential_dims(
      const bool is_first_block,
      const ObDatum &block_start_id,
      const uint64_t block_end_id,
      bool &has_valid_doc_in_block);
  int block_merge_non_essential_dims(ObDatum &doc_id_datum);
  bool need_update_essential_dims() const { return non_essential_dim_threshold_ < get_top_k_threshold(); }
private:
  int calc_other_dims_max_score_sum(const int64_t iter_idx, double &max_score_sum);
  int set_filter_threshold_for_dim(const int64_t iter_idx, ObISRDimBlockMaxIter *iter);
  int update_filter_thresholds_after_topk_update();
  class DimMaxScoreCmp
  {
  public:
    explicit DimMaxScoreCmp(const ObIArray<ObISRDaaTDimIter *> &dim_iters, int &ret)
      : dim_iters_(dim_iters), ret_(ret)
    {}
    bool operator()(const int64_t lhs, const int64_t rhs) const
    {
      int ret = OB_SUCCESS;
      double score1 = 0.0;
      double score2 = 0.0;
      if (OB_FAIL(dim_iters_.at(lhs)->get_dim_max_score(score1))) {
        STORAGE_LOG(WARN, "failed to get dim max score", K(ret));
      } else if (OB_FAIL(dim_iters_.at(rhs)->get_dim_max_score(score2))) {
        STORAGE_LOG(WARN, "failed to get dim max score", K(ret));
      }
      ret_ = ret;
      return score1 < score2;
    }

  private:
    const ObIArray<ObISRDaaTDimIter *> &dim_iters_;
    int &ret_;
  };

private:
  ObFixedArray<int64_t, ObIAllocator> sorted_iters_;
  int64_t non_essential_dim_count_;
  double non_essential_dim_max_score_;
  double non_essential_dim_threshold_;
  double all_dims_max_score_sum_;
  ObFixedArray<double, ObIAllocator> dim_max_scores_;
  int64_t block_merge_bucket_size_;
  uint64_t block_merge_upper_bound_;
  int64_t block_merge_doc_cnt_;
  ObFixedArray<double, ObIAllocator> block_merge_scores_;
  ObFixedArray<uint64_t, ObIAllocator> block_merge_doc_ids_;
  bool can_use_block_merge_;
  bool is_max_score_cached_;
  DISALLOW_COPY_AND_ASSIGN(ObSRBMMIterImpl);
};

} // namespace storage
} // namespace oceanbase

#endif
