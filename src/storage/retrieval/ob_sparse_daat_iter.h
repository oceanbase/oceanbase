/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_SPARSE_DAAT_ITER_H_
#define OB_SPARSE_DAAT_ITER_H_

#include "ob_i_sparse_retrieval_iter.h"
#include "ob_sparse_retrieval_util.h"
#include "ob_sparse_utils.h"
#include "lib/container/ob_loser_tree.h"
#include "lib/container/ob_heap.h"
#include "sql/das/ob_das_ir_define.h"

namespace oceanbase
{
namespace storage
{

struct ObSRMergeItem
{
  ObSRMergeItem() : relevance_(0.0), iter_idx_(-1) {}
  ~ObSRMergeItem() = default;
  TO_STRING_KV(K_(iter_idx), K_(relevance));

  double relevance_;
  int64_t iter_idx_;
};

struct ObSRMergeCmp
{
  ObSRMergeCmp();
  virtual ~ObSRMergeCmp() {}

  int init(ObDatumMeta id_meta, const ObFixedArray<const ObDatum *, ObIAllocator> *iter_ids);
  int cmp(const ObSRMergeItem &l, const ObSRMergeItem &r, int64_t &cmp_ret);
private:
  inline const ObDatum &get_id_datum(const int64_t iter_idx)
  {
    const ObDatum *datum = iter_ids_->at(iter_idx);
    OB_ASSERT(nullptr != datum);
    return *datum;
  }
private:
  common::ObDatumCmpFuncType cmp_func_;
  // TODO: if memory lifetime of docid datum is guaranteed by dim_iters, we can use pointer to datum directly
  //       and avoid deep copy into merge heap here
  const ObFixedArray<const ObDatum *, ObIAllocator> *iter_ids_;
  bool is_inited_;
};

typedef ObMergeLoserTree<ObSRMergeItem, ObSRMergeCmp> ObSRMergeLoserTree;

// implementation of basic DaaT query processing algorithm primitives
class ObSRDaaTIterImpl : public ObISparseRetrievalMergeIter
{
public:
  ObSRDaaTIterImpl();
  virtual ~ObSRDaaTIterImpl() {}
  virtual int get_next_row() override;
  virtual int get_next_rows(const int64_t capacity, int64_t &count) override;
  int init(
      ObSparseRetrievalMergeParam &iter_param,
      ObIArray<ObISRDaaTDimIter *> &dim_iters,
      ObIAllocator &iter_allocator,
      ObSRDaaTRelevanceCollector &relevance_collector);
  virtual void reuse(const bool switch_tablet = false) override;
  virtual void reset() override;
  virtual int get_query_max_score(double &score) override;

  INHERIT_TO_STRING_KV("ObISparseRetrievalMergeIter", ObISparseRetrievalMergeIter,
      K_(next_round_iter_idxes), K_(next_round_cnt));
protected:
  virtual int pre_process();
  virtual int do_one_merge_round(int64_t &count);
  virtual int fill_merge_heap();
  virtual int collect_dims_by_id(const ObDatum *&id_datum, double &relevance, bool &got_valid_id);
  virtual int process_collected_row(const ObDatum &id_datum, const double relevance);
  virtual int filter_on_demand(const int64_t count, const double relevance, bool &need_project);
  virtual int cache_result(int64_t &count, const ObDatum &id_datum, const double relevance);
  virtual int project_results(const int64_t count);
protected:
  ObIAllocator *iter_allocator_;
  ObSparseRetrievalMergeParam *iter_param_;
  ObIArray<ObISRDaaTDimIter *> *dim_iters_;
  ObSRMergeCmp merge_cmp_;
  ObSRMergeLoserTree *merge_heap_;
  ObSRDaaTRelevanceCollector *relevance_collector_;
  ObFixedArray<const ObDatum *, ObIAllocator> iter_domain_ids_; // record every dim iter's output domain id, one (ObDatum *) for one dim iter
  ObFixedArray<ObDocIdExt, ObIAllocator> buffered_domain_ids_; // cache for output
  ObFixedArray<double, ObIAllocator> buffered_relevances_;
  ObFixedArray<int64_t, ObIAllocator> next_round_iter_idxes_;
  int64_t next_round_cnt_;
  void (*set_datum_func_)(ObDatum &, const ObDocIdExt &);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSRDaaTIterImpl);
};

class ObSRBlockMaxTopKIterImpl : public ObSRDaaTIterImpl
{
public:
  ObSRBlockMaxTopKIterImpl();
  virtual ~ObSRBlockMaxTopKIterImpl() {}
  virtual void reuse(const bool switch_tablet = false) override;
  void reset();
  int init(
      ObSparseRetrievalMergeParam &iter_param,
      ObIArray<ObISRDaaTDimIter *> &dim_iters,
      ObIAllocator &iter_allocator,
      ObSRDaaTRelevanceCollector &relevance_collector);
  virtual int get_next_rows(const int64_t capacity, int64_t &count) override;
  virtual int preset_top_k_threshold(const double threshold) override;
protected:
  virtual int process_collected_row(const ObDatum &id_datum, const double relevance) override;
  virtual int top_k_search() { return OB_NOT_IMPLEMENT; }
  virtual int init_before_topk_search() { return OB_NOT_IMPLEMENT; }
  int before_top_k_process();
  int build_top_k_heap();
  int next_pivot_range(const double &score_threshold, int64_t &skip_range_cnt);
  int advance_dim_iters_for_next_round(const ObDatum &target_id, const bool iter_end_available);
  double get_top_k_threshold() const
  {
    return  MAX(top_k_heap_.empty() ? 0.0 : top_k_heap_.top().relevance_, initial_top_k_threshold_);
  }
  ObISRDimBlockMaxIter *get_iter(const int64_t iter_idx) const
  {
    return static_cast<ObISRDimBlockMaxIter *>(dim_iters_->at(iter_idx));
  }
private:
  int project_rows_from_top_k_heap(const int64_t capacity, int64_t &count);
  int fill_merge_heap_with_shallow_dims(const ObDatum *last_range_border_id, const bool inclusive);
  int try_generate_next_range_from_merge_heap(
      const double &score_threshold,
      bool &is_candidate_range,
      const ObDatum *&min_domain_id_with_pivot,
      const ObDatum *&max_domain_id_without_pivot);
protected:
  enum BMSearchStatus{
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
protected:
  ObArenaAllocator allocator_;
  TopKItemCmp less_score_cmp_;
  // Maybe domain id not need to be cached within the topk heap since there would be top-n sort with datum store above
  TopKHeap top_k_heap_;
  int64_t top_k_count_;
  ObDomainIdCmp domain_id_cmp_;
  ObFixedArray<ObDocIdExt, ObIAllocator> id_cache_;
  double initial_top_k_threshold_;
  BMSearchStatus status_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSRBlockMaxTopKIterImpl);
};

} // namespace storage
} // namespace oceanbase

#endif
