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

#ifndef OB_SPIV_DAAT_ITER_H_
#define OB_SPIV_DAAT_ITER_H_

#include "ob_sparse_daat_iter.h"
#include "ob_sparse_bmw_iter.h"
#include "ob_spiv_dim_iter.h"

namespace oceanbase
{
namespace sql
{
class ObDASScanIter;
}
namespace storage
{

struct ObSPIVDaaTParam
{
  ObSPIVDaaTParam()
      : dim_iters_(nullptr),
        base_param_(),
        allocator_(nullptr),
        relevance_collector_(nullptr),
        is_pre_filter_(false),
        is_use_docid_(true)
  {}
  ~ObSPIVDaaTParam()
  {}
  TO_STRING_KV(K_(base_param), KP_(dim_iters), K_(is_pre_filter));
  ObIArray<ObISRDaaTDimIter *> *dim_iters_;
  ObSparseRetrievalMergeParam *base_param_;
  common::ObArenaAllocator *allocator_;
  ObSRDaaTRelevanceCollector *relevance_collector_;
  bool is_pre_filter_;
  bool is_use_docid_;
};

struct ObDocidSortItem
{
  ObDocidSortItem() = default;
  ObDocidSortItem(int64_t doc_idx, double score): doc_idx_(doc_idx), score_(score) {}
  ~ObDocidSortItem() = default;
  int64_t doc_idx_;
  double score_;
  uint64_t iter_idx_;
  TO_STRING_KV(K_(doc_idx), K_(score), K_(iter_idx));
};

struct ObDocidSortItemCmp
{
public:
  bool operator() (const ObDocidSortItem &a, const ObDocidSortItem &b)
  {
    return a.score_ < b.score_;
  }

  int get_error_code() { return common::OB_SUCCESS; }
};

template <typename T,
          typename CompareFunctor>
class ObSPIVSortHeap {
public:
  ObSPIVSortHeap(int64_t limit_size, ObIAllocator &allocator, CompareFunctor cmp)
      : limit_size_(limit_size), allocator_(allocator), cmp_(cmp), heap_(cmp, &allocator_), doc_ids_array_()
  {
    doc_ids_array_.set_attr(ObMemAttr(MTL_ID(), "ObSPIVSortHeap"));
  }

  ~ObSPIVSortHeap() = default;

  int add_doc_id(const ObDocIdExt &doc_id) {
    return doc_ids_array_.push_back(doc_id);
  }

  const ObDocIdExt &get_doc_id(int64_t idx) const {
    return doc_ids_array_.at(idx);
  }

  int push(const ObDocIdExt &doc_id, double score)
  {
    int ret = OB_SUCCESS;
    ObDocidSortItem item;
    item.score_ = score;
    int64_t doc_idx;
    if (OB_FAIL(add_doc_id(doc_id))) {
      SHARE_LOG(WARN, "failed to add doc id", K(ret));
    } else if (OB_FALSE_IT(doc_idx = doc_ids_count() - 1)) {
    } else if (OB_FALSE_IT(item.doc_idx_ = doc_idx)) {
    } else if (heap_.count() < limit_size_) {
      if (OB_FAIL(heap_.push(item))) {
        SHARE_LOG(WARN, "failed to push heap", K(ret));
      }
    } else if (cmp_(item, heap_.top())) {
      if (OB_FAIL(heap_.pop())) {
        SHARE_LOG(WARN, "failed to pop heap", K(ret));
      } else if (OB_FAIL(heap_.push(item))) {
        SHARE_LOG(WARN, "failed to push heap", K(ret));
      }
    }

    return ret;
  }
  int pop() { return heap_.pop(); }
  int64_t count() { return heap_.count(); }
  int64_t doc_ids_count() { return doc_ids_array_.count(); }
  T& top() { return heap_.top(); }
  bool empty() { return heap_.empty(); }
private:
  int64_t limit_size_;
  ObArenaAllocator allocator_;
  CompareFunctor cmp_;
  ObBinaryHeap<T, CompareFunctor, 16> heap_;
  common::ObSEArray<ObDocIdExt, 16> doc_ids_array_;
};

typedef ObSPIVSortHeap<ObDocidSortItem, ObDocidSortItemCmp> SPIVSortHeap;

class ObSPIVDaaTIter : public ObSRDaaTIterImpl
{
public:
  ObSPIVDaaTIter()
      : ObSRDaaTIterImpl(),
        is_pre_filter_(false),
        is_use_docid_(false),
        sort_heap_(nullptr),
        result_docids_(),
        result_docids_curr_iter_(OB_INVALID_INDEX_INT64)
  {
    result_docids_.set_attr(ObMemAttr(MTL_ID(), "SPIVResultDocid"));
  }
  virtual ~ObSPIVDaaTIter();
  int init(const ObSPIVDaaTParam &param);
  virtual int inner_init(const ObSPIVDaaTParam &param);
  int set_valid_docid_set(const common::hash::ObHashSet<ObDocIdExt> &valid_docid_set);
  void reset();
  virtual void reuse() override;
  virtual void inner_reuse()
  {}
  virtual void inner_reset(){}
  virtual int pre_process() override;
  virtual int get_next_row() override;
  virtual int inner_get_next_row();
  virtual int get_next_rows(const int64_t capacity, int64_t &count) override;
  virtual int inner_get_next_rows(const int64_t capacity, int64_t &count);
  virtual int process();
  virtual void set_ls_related_tablet_id(const share::ObLSID &ls_id, const ObDASRelatedTabletID &related_tablet_ids)
  {}

protected:
  // for pre-filter
  common::hash::ObHashSet<ObDocIdExt> valid_docid_set_;
  bool is_pre_filter_;
  bool is_use_docid_;
  ObDocidSortItemCmp docid_score_cmp_;
  // for sort
  SPIVSortHeap *sort_heap_;
  common::ObSEArray<ObDocIdExt, 16> result_docids_;
  int64_t result_docids_curr_iter_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSPIVDaaTIter);
};

class ObSPIVDaaTNaiveIter : public ObSPIVDaaTIter
{
public:
  ObSPIVDaaTNaiveIter() : ObSPIVDaaTIter()
  {}
  virtual ~ObSPIVDaaTNaiveIter()
  {}
  virtual int inner_init(const ObSPIVDaaTParam &param) override;
  virtual void inner_reuse() override;
  virtual void inner_reset() override;
  virtual int process() override;

  // push result to sort_heap
  virtual int project_results(int64_t count) override;

  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(const int64_t capacity, int64_t &count) override;

};

class ObSPIVBMWIter final : public ObSRBMWIterImpl
{
public:
  ObSPIVBMWIter() : is_pre_filter_(false), is_use_docid_(true), result_docids_(), result_docids_curr_iter_(-1)
  {
    result_docids_.set_attr(ObMemAttr(MTL_ID(), "SPIVResultDocid"));
  }
  virtual ~ObSPIVBMWIter() { reset(); }
  virtual void reuse() override;
  void reset();
  int init(const ObSPIVDaaTParam &param);
  virtual int get_next_row() override;
  virtual int get_next_rows(const int64_t capacity, int64_t &count) override;
  virtual int init_before_wand_process() override;
  // do pre-filter
  virtual int process_collected_row(const ObDatum &id_datum, const double relevance) override;
  int project_rows_from_result_docids(const int64_t capacity, int64_t &count);
  int reverse_top_k_heap();
  int set_valid_docid_set(const common::hash::ObHashSet<ObDocIdExt> &valid_docid_set);

protected:
  // for pre-filter
  common::hash::ObHashSet<ObDocIdExt> valid_docid_set_;
  bool is_pre_filter_;
  bool is_use_docid_;
  common::ObSEArray<ObDocIdExt, 16> result_docids_;
  int64_t result_docids_curr_iter_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSPIVBMWIter);
};

}  // namespace storage
}  // namespace oceanbase

#endif
