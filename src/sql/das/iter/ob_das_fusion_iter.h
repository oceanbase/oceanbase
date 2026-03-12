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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_FUSION_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_FUSION_ITER_H_

#include "share/ob_i_tablet_scan.h"
#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/search/ob_das_search_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_heap.h"
#include "lib/hash/ob_hashmap.h"
#include "common/rowkey/ob_rowkey.h"
#include "share/hybrid_search/ob_query_parse.h"
#include <cstdint>

namespace oceanbase
{
namespace sql
{

/**
 * Document info for fusion scoring with multi-path aggregation
 */
struct ObDASFusionDocInfo
{
  union RowkeyUnion {
    uint64_t uint64_val_;
    common::ObRowkey rowkey_;

    RowkeyUnion() : uint64_val_(0) {}
    ~RowkeyUnion() {}
  };

  ObDASFusionDocInfo()
    : fusion_score_(0.0),
      raw_scores_(),
      rowkey_union_(),
      is_uint64_rowkey_(false),
      path_bitmap_(0),
      path_count_(0)
  {
    rowkey_union_.uint64_val_ = 0;
  }

  ~ObDASFusionDocInfo()
  {
    if (!is_uint64_rowkey_) {
      rowkey_union_.rowkey_.~ObRowkey();
    }
  }

  void reset()
  {
    fusion_score_ = 0.0;
    raw_scores_.destroy();
    rank_scores.destroy();
    if (!is_uint64_rowkey_) {
      rowkey_union_.rowkey_.reset();
    } else {
      rowkey_union_.uint64_val_ = 0;
    }
    is_uint64_rowkey_ = false;
    path_bitmap_ = 0;
    path_count_ = 0;
  }

  // Set uint64 rowkey (fast path)
  void set_uint64_rowkey(uint64_t val)
  {
    if (!is_uint64_rowkey_) {
      rowkey_union_.rowkey_.~ObRowkey();
      is_uint64_rowkey_ = true;
    }
    rowkey_union_.uint64_val_ = val;
  }

  OB_INLINE void set_rowkey(const common::ObRowkey &rowkey)
  {
    if (is_uint64_rowkey_) {
      new (&rowkey_union_.rowkey_) common::ObRowkey();
      is_uint64_rowkey_ = false;
    }
    rowkey_union_.rowkey_ = rowkey;
  }

  uint64_t get_uint64_rowkey() const
  {
    return rowkey_union_.uint64_val_;
  }

  OB_INLINE bool is_uint64() const { return is_uint64_rowkey_; }

  int init_raw_scores(int64_t path_count, common::ObArenaAllocator &allocator, ObFusionMethod fusion_method);

  OB_INLINE bool has_path(int64_t path_idx) const
  {
    return path_idx < 64 && (path_bitmap_ & (1ULL << path_idx));
  }

  OB_INLINE void set_path(int64_t path_idx)
  {
    if (path_idx < 64) {
      if (!has_path(path_idx)) {
        path_bitmap_ |= (1ULL << path_idx);
        ++path_count_;
      }
    }
  }

  int get_raw_score(int64_t path_idx, double &score) const;
  int set_raw_score(int64_t path_idx, double score);

  TO_STRING_KV(K_(fusion_score), K_(is_uint64_rowkey), K_(path_bitmap), K_(path_count), K_(raw_scores));

  double fusion_score_;
  common::ObFixedArray<double, common::ObArenaAllocator> raw_scores_;
  common::ObFixedArray<int64_t, common::ObArenaAllocator> rank_scores;
  RowkeyUnion rowkey_union_;
  bool is_uint64_rowkey_;
  uint64_t path_bitmap_;
  int64_t path_count_;
};

/**
 * Entry for Top-K heap and RRF ranking
 */
struct ObDASFusionScoreEntry
{
public:
  ObDASFusionScoreEntry() : doc_idx_(-1), score_(0.0) {}
  ObDASFusionScoreEntry(int64_t doc_idx, double score) : doc_idx_(doc_idx), score_(score) {}

  bool operator<(const ObDASFusionScoreEntry &other) const
  {
    return score_ > other.score_;
  }

  TO_STRING_KV(K_(doc_idx), K_(score));

public:
  int64_t doc_idx_;
  double score_;
};

/**
 * Comparator for min-heap (smallest score at top)
 */
struct ObDASFusionScoreMinHeapCmp
{
  ObDASFusionScoreMinHeapCmp() : fusion_docs_(nullptr), rowkey_is_uint64_(false) {}
  ObDASFusionScoreMinHeapCmp(const common::ObIArray<ObDASFusionDocInfo> *fusion_docs, bool rowkey_is_uint64)
    : fusion_docs_(fusion_docs), rowkey_is_uint64_(rowkey_is_uint64) {}

  int get_error_code() { return OB_SUCCESS; }

  bool operator()(const ObDASFusionScoreEntry &l, const ObDASFusionScoreEntry &r)
  {
    if (l.score_ > r.score_) {
      return true;
    } else if (l.score_ < r.score_) {
      return false;
    } else {
      if (rowkey_is_uint64_) {
        uint64_t rowkey_l = fusion_docs_->at(l.doc_idx_).get_uint64_rowkey();
        uint64_t rowkey_r = fusion_docs_->at(r.doc_idx_).get_uint64_rowkey();
        return rowkey_l < rowkey_r;
      } else {
        const common::ObRowkey &rowkey_l = fusion_docs_->at(l.doc_idx_).rowkey_union_.rowkey_;
        const common::ObRowkey &rowkey_r = fusion_docs_->at(r.doc_idx_).rowkey_union_.rowkey_;
        return rowkey_l.compare(rowkey_r) < 0;
      }
    }
  }

private:
  const common::ObIArray<ObDASFusionDocInfo> *fusion_docs_;
  bool rowkey_is_uint64_;
};

typedef common::ObBinaryHeap<ObDASFusionScoreEntry, ObDASFusionScoreMinHeapCmp, 64> ObDASFusionScoreHeap;

struct ObDASFusionIterParam : public ObDASIterParam
{
public:
  ObDASFusionIterParam()
    : ObDASIterParam(ObDASIterType::DAS_ITER_FUSION),
      fusion_ctdef_(nullptr),
      fusion_method_(ObFusionMethod::WEIGHT_SUM),
      fusion_score_expr_(nullptr),
      rank_window_size_(10),
      rank_constant_(60),
      size_(10),
      offset_(0),
      min_score_(0.0),
      has_hybrid_fusion_op_(false)
  {}
  virtual ~ObDASFusionIterParam() {}

  virtual bool is_valid() const override
  {
    return ObDASIterParam::is_valid() && nullptr != fusion_ctdef_;
  }

  const ObDASFusionCtDef *fusion_ctdef_;
  ObFusionMethod fusion_method_;
  ObExpr *fusion_score_expr_;
  int64_t rank_window_size_;
  int64_t rank_constant_;
  int64_t size_;
  int64_t offset_;
  double min_score_;
  bool has_hybrid_fusion_op_;
  common::ObSEArray<double, 2> weights_;
  common::ObSEArray<int64_t, 2> path_top_k_limits_;
};

/**
 * ObDASFusionIter - DAS layer iterator for multi-path fusion
 *
 * Responsibilities:
 * - Manage child iterators (multiple recall paths)
 * - Collect data from children and aggregate by rowkey
 * - Build Top-K heap and sort results
 * - Provide iterator interface (get_next_row/get_next_rows)
 *
 * Workflow:
 * 1. do_table_scan(): Trigger table scan on all children
 * 2. inner_get_next_row(): First call triggers do_fusion() to collect all data
 * 3. do_fusion(): Iterate all children, add rows and aggregate scores
 * 4. finish_fusion(): Build Top-K heap and sort
 * 5. get_next_row_from_store(): Return sorted results
 */
// Forward declaration for friend class
class ObDASFusionIterTest;

class ObDASFusionIter : public ObDASIter
{
  friend class ObDASFusionIterTest;
public:
  ObDASFusionIter()
    : ObDASIter(ObDASIterType::DAS_ITER_FUSION),
      fusion_memctx_(nullptr),
      fusion_ctdef_(nullptr),
      score_exprs_(nullptr),
      fusion_method_(ObFusionMethod::WEIGHT_SUM),
      rank_window_size_(10),
      rank_constant_(60),
      size_(10),
      offset_(0),
      min_score_(0.0),
      has_hybrid_fusion_op_(false),
      fusion_row_inited_(false),
      fusion_finished_(false),
      fake_skip_(nullptr),
      fusion_score_heap_(nullptr),
      rowid_map_inited_(false),
      uint64_rowid_map_inited_(false),
      is_sorted_(false),
      rowkey_is_uint64_(false),
      output_idx_(0),
      input_row_cnt_(0),
      output_row_cnt_(0)
  {}
  virtual ~ObDASFusionIter() {}

public:
  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;
  OB_INLINE int64_t get_path_count() const { return fusion_ctdef_->children_cnt_; }

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  int init_fusion_row();
  int do_fusion(bool is_vectorized);
  int finish_fusion();

  // Row ID extraction and lookup
  int extract_score(const int64_t path_idx, double &score);

  template<typename RowkeyType>
  int find_or_create_doc(RowkeyType &rowkey,
                         const int64_t path_idx,
                         double score);

  // Top-K heap operations
  int init_fusion_score_heap();
  void free_fusion_score_heap();
  int build_topk_heap();
  int sort_topk_results();

  // rowid map operations
  int init_rowid_map();
  void destroy_rowid_map();

  int calculate_fusion_score();
  int calculate_weight_sum_score();
  int calculate_minmax_normalizer_score();
  int set_score_to_doc(int64_t doc_idx, double score, int64_t path_idx);
  int set_score_to_expr(const ObDASFusionDocInfo &doc);

  // using for distributed fusion, and method is minmax_normalizer
  int build_all_path_topk_doc_indices();

  /// calculate RRF
  int calculate_rrf_score();

  template<typename RowkeyType>
  int extract_rowkey(RowkeyType &rowkey);
  template<>
  int extract_rowkey<uint64_t>(uint64_t &rowkey);

  template<typename RowkeyType>
  int set_rowkey_to_exprs(const RowkeyType &rowkey, const ObIArray<ObExpr *> &rowkey_exprs);
  template<>
  int set_rowkey_to_exprs<uint64_t>(const uint64_t &rowkey, const ObIArray<ObExpr *> &rowkey_exprs);

  int set_rowkey_and_score_to_exprs(int64_t doc_idx);
  int set_rowkey_and_score_to_exprs_batch(int64_t start_idx,
                                          int64_t batch_size);
  template<typename RowkeyType>
  int set_rowkeys_batch_rich_format(int64_t start_idx,
                                    int64_t batch_size);
  template<>
  int set_rowkeys_batch_rich_format<uint64_t>(int64_t start_idx,
                                              int64_t batch_size);

  template<typename RowkeyType>
  int set_rowkeys_batch_non_rich_format(int64_t start_idx,
                                         int64_t batch_size);
  template<>
  int set_rowkeys_batch_non_rich_format<uint64_t>(int64_t start_idx,
                                                  int64_t batch_size);

  int set_scores_batch_rich_format(int64_t start_idx,
                                   int64_t batch_size);
  int set_scores_batch_non_rich_format(int64_t start_idx,
                                       int64_t batch_size);

  static int init_output_vectors(
      const common::ObIArray<ObExpr *> &exprs,
      ObEvalCtx &ctx,
      const int64_t size);

  int add_row(const int64_t path_idx, const common::ObIArray<ObExpr *> &exprs);
  int add_batch(const int64_t path_idx, const common::ObIArray<ObExpr *> &exprs, const int64_t batch_size);

  // Get next row/batch from sorted results
  int get_next_row_from_store(const common::ObIArray<ObExpr *> &exprs);
  int get_next_batch_from_store(const common::ObIArray<ObExpr *> &exprs, int64_t capacity, int64_t &count);

  int set_rowkey_is_uint64_flag();
  bool need_sorted() const { return !(has_hybrid_fusion_op_ && (fusion_method_ == MINMAX_NORMALIZER)); }
  bool has_hybrid_fusion_op() const { return has_hybrid_fusion_op_; }

private:
  lib::MemoryContext fusion_memctx_;
  const ObDASFusionCtDef *fusion_ctdef_;
  // Score expressions array: [path0_score, path1_score, ..., fusion_score]
  // The order matches children_ array order, last element is fusion_score
  const ExprFixedArray *score_exprs_;
  // Fusion parameters (extracted from ObDASFusionIterParam to avoid copying entire struct)
  ObFusionMethod fusion_method_;
  int64_t rank_window_size_;
  int64_t rank_constant_;
  int64_t size_;
  int64_t offset_;
  double min_score_;
  bool has_hybrid_fusion_op_;
  common::ObSEArray<double, 2> weights_;
  common::ObSEArray<int64_t, 2> path_top_k_limits_;

  // Expressions to store in row_store
  common::ObSEArray<ObExpr *, 8> fusion_row_;
  bool fusion_row_inited_;
  bool fusion_finished_;
  ObBitVector *fake_skip_;

  common::ObSEArray<ObDASFusionDocInfo, 64> fusion_docs_;
  ObDASFusionScoreMinHeapCmp heap_cmp_;
  ObDASFusionScoreHeap *fusion_score_heap_;
  common::ObSEArray<int64_t, 64> sorted_doc_indices_;

  // Row ID to doc_idx mapping
  typedef common::hash::ObHashMap<common::ObRowkey, int64_t,
                                   common::hash::NoPthreadDefendMode> RowIdDocMap;
  RowIdDocMap rowid_doc_map_;
  bool rowid_map_inited_;

  typedef common::hash::ObHashMap<uint64_t, int64_t,
                                   common::hash::NoPthreadDefendMode> Uint64RowIdDocMap;
  Uint64RowIdDocMap uint64_rowid_doc_map_;
  bool uint64_rowid_map_inited_;

  // State
  bool is_sorted_;
  bool rowkey_is_uint64_;
  int64_t output_idx_;

  int64_t input_row_cnt_;
  int64_t output_row_cnt_;
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_FUSION_ITER_H_ */
