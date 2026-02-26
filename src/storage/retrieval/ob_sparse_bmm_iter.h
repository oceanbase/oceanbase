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
  double get_essential_dim_threshold() const { return get_top_k_threshold() - non_essential_dim_max_score_; }
  int sort_and_classify_dims();
  int try_update_essential_dims();
  int next_pivot(ObDocIdExt &pivot_id);
  int evaluate_pivot_range(const ObDatum &pivot_id, double &non_essential_block_max_score, bool &is_candidate);
  int evaluate_essential_pivot(
      const ObDatum &pivot_id,
      const double non_essential_block_max_score,
      double &essential_score,
      bool &is_candidate,
      bool &is_valid_pivot);
  int evaluate_pivot(const ObDatum &pivot_id, const double &essential_score);
  int forward_next_round_iters();
  bool need_update_essential_dims() const { return non_essential_dim_threshold_ < get_top_k_threshold(); }
private:
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
  DISALLOW_COPY_AND_ASSIGN(ObSRBMMIterImpl);
};

} // namespace storage
} // namespace oceanbase

#endif
