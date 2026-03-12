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

#ifndef OB_DISJUNCTIVE_MAX_ITER_H_
#define OB_DISJUNCTIVE_MAX_ITER_H_

#include "lib/container/ob_heap.h"
#include "lib/container/ob_se_array.h"
#include "ob_sparse_bmw_iter.h"

namespace oceanbase
{
namespace storage
{

struct ObSRDisjunctiveMaxIterParam
{
  ObSRDisjunctiveMaxIterParam()
    : es_match_ctdef_(nullptr),
      es_match_rtdef_(nullptr),
      ir_scan_ctdefs_(nullptr),
      ir_scan_rtdefs_(nullptr),
      topk_limit_(0)
  {}

  const ObDASIREsMatchCtDef *es_match_ctdef_;
  ObDASIREsMatchRtDef *es_match_rtdef_;
  ObIArray<const ObDASIRScanCtDef *> *ir_scan_ctdefs_;
  ObIArray<ObDASIRScanRtDef *> *ir_scan_rtdefs_;
  int64_t topk_limit_;
};

class ObSRDisjunctiveMaxIter final : public ObISparseRetrievalMergeIter
{
public:
  ObSRDisjunctiveMaxIter();
  virtual ~ObSRDisjunctiveMaxIter() {}
  virtual int get_next_row() override;
  virtual int get_next_rows(const int64_t capacity, int64_t &count) override;
  int init(
      ObSRDisjunctiveMaxIterParam &iter_param,
      ObIArray<ObSRBlockMaxTopKIterImpl *> &topk_iters,
      ObIAllocator &iter_allocator);
  virtual void reuse(const bool switch_tablet = false) override;
  virtual void reset() override;
  INHERIT_TO_STRING_KV("ObISparseRetrievalMergeIter", ObISparseRetrievalMergeIter,
    K_(topk_limit));
protected:
  int load_results();
  int project_results(const int64_t capacity, int64_t &count);
protected:
  struct TopKItem
  {
    TopKItem() : relevance_(0.0), cache_idx_(-1) {}
    TopKItem(const double relevance, const int64_t idx) : relevance_(relevance), cache_idx_(idx) {}
    ~TopKItem() = default;
    TO_STRING_KV(K_(relevance), K_(cache_idx));
    double relevance_;
    int64_t cache_idx_;
  };
  struct TopKItemCmp
  {
    bool operator()(const TopKItem &a, const TopKItem &b) const
    {
      return a.relevance_ > b.relevance_;
    }
    int get_error_code() { return OB_SUCCESS; }
  };
  typedef common::ObBinaryHeap<TopKItem, TopKItemCmp> TopKHeap;
  typedef hash::ObHashMap<ObDocIdExt, TopKItem> TopKHashMap;
  ObIAllocator *iter_allocator_;
  ObFixedArray<ObSRBlockMaxTopKIterImpl *, ObIAllocator> topk_iters_;
  const ObDASIREsMatchCtDef *es_match_ctdef_;
  ObDASIREsMatchRtDef *es_match_rtdef_;
  ObIArray<const ObDASIRScanCtDef *> *ir_scan_ctdefs_;
  ObIArray<ObDASIRScanRtDef *> *ir_scan_rtdefs_;
  int64_t topk_limit_;
  double norm_;
  TopKItemCmp score_cmp_;
  TopKHeap heap_;
  TopKHashMap hash_map_;
  common::ObSEArray<ObDocIdExt, 64> id_cache_;
  void (*set_datum_func_)(ObDatum &, const ObDocIdExt &);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSRDisjunctiveMaxIter);
};

} // namespace storage
} // namespace oceanbase

#endif
