/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_BASIC_OB_HYBRID_FUSION_OP_H_
#define OCEANBASE_BASIC_OB_HYBRID_FUSION_OP_H_

#include "lib/hash/ob_hashset.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_heap.h"
#include "share/hybrid_search/ob_query_parse.h"
#include "sql/optimizer/ob_log_link.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"

// Forward declaration for unit test friend classes
namespace oceanbase { namespace sql { class ObHybridFusionOpTestHelper; } }
namespace oceanbase { namespace sql { class ObHybridFusionOpE2EHelper; } }
namespace oceanbase { namespace sql { class ObHybridFusionOpCollectTestHelper; } }

namespace oceanbase
{
namespace common
{
class ObJsonNode;
}
namespace sql
{
struct ObFusionDocInfo
{
  ObFusionDocInfo() : row_store_idx_(-1), fusion_score_(0.0) {}
  ~ObFusionDocInfo() { reset(); }

  int64_t row_store_idx_;
  double fusion_score_;
  void reset() {
    row_store_idx_ = -1;
    fusion_score_ = 0.0;
  }

  TO_STRING_KV(K_(row_store_idx), K_(fusion_score));
};

struct ObPathStats
{
  double min_score_;
  double max_score_;

  ObPathStats() : min_score_(DBL_MAX), max_score_(-DBL_MAX) {}

  void reset() {
    min_score_ = DBL_MAX;
    max_score_ = -DBL_MAX;
  }

  TO_STRING_KV(K_(min_score), K_(max_score));
};

// top-k heap element
// doc_idx_ is the index of the document in the fusion_docs_ array
// score_ is the score of the document
struct ObPathScoreEntry
{
  int64_t doc_idx_;
  double score_;
  ObPathScoreEntry() : doc_idx_(-1), score_(0.0) {}
  ObPathScoreEntry(int64_t idx, double s) : doc_idx_(idx), score_(s) {}

  TO_STRING_KV(K_(doc_idx), K_(score));
};

class ObScoreEntryCompare
{
public:
  ObScoreEntryCompare() : ret_(common::OB_SUCCESS) {}

  bool operator()(const ObPathScoreEntry &left, const ObPathScoreEntry &right)
  {
    return left.score_ > right.score_;
  }

  int get_error_code() const { return ret_; }

  TO_STRING_KV(K_(ret));

private:
  int ret_;
};

typedef common::ObBinaryHeap<ObPathScoreEntry, ObScoreEntryCompare, 10> ObTopKHeap;

// AI rerank params for hybrid fusion (runtime spec); all nullptr / -1 when not used.
struct ObHybridFusionRerankSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObHybridFusionRerankSpec()
    : has_rerank_(false), model_key_expr_(nullptr), query_expr_(nullptr),
      field_idx_(-1), window_size_expr_(nullptr) {}
  bool has_rerank_;
  ObExpr *model_key_expr_;
  ObExpr *query_expr_;
  int64_t field_idx_;  // index of text column in child output
  ObExpr *window_size_expr_;

  TO_STRING_KV(K_(has_rerank), K_(field_idx));
};

class ObHybridFusionSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObHybridFusionSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  ~ObHybridFusionSpec() {}

  ObFusionMethod fusion_method_;
  ObExpr *min_score_expr_;
  ObExpr *rank_window_size_expr_;
  ObExpr *rank_constant_expr_;
  ObExpr *size_expr_;
  ObExpr *offset_expr_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> weights_exprs_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> score_exprs_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> path_top_k_limit_exprs_;
  common::ObFixedArray<int64_t, common::ObIAllocator> score_expr_output_indices_;

  int64_t search_index_;
  ObFusionIterExecMode fusion_iter_exec_mode_;
  ObHybridFusionRerankSpec rerank_spec_;
  bool is_single_partition_;
};

class ObHybridFusionOp : public ObOperator
{
public:
  enum FullRecallPhase { PHASE_STREAMING = 0, PHASE_OUTPUT_KNN = 1 };

  ObHybridFusionOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input) :
    ObOperator(exec_ctx, spec, input), spec_(static_cast<const ObHybridFusionSpec&>(spec)),
    is_data_ready_(false), top_k_limit_(10), size_(10), offset_(0), min_score_(0.0),
    rank_constant_(0), output_idx_(0), path_count_(0), fusion_iter_exec_mode_(ObFusionIterExecMode::SCORE_TOP_K_QUERY_HITS),
    search_index_(-1), full_recall_phase_(PHASE_STREAMING), rerank_window_size_(0),
    profile_(ObSqlWorkAreaType::HASH_WORK_AREA), sql_mem_processor_(profile_, op_monitor_info_) {}

  virtual ~ObHybridFusionOp() {}

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;

  bool is_weight_sum() const { return spec_.fusion_method_ == ObFusionMethod::WEIGHT_SUM; }
  bool is_rrf() const { return spec_.fusion_method_ == ObFusionMethod::RRF; }
  bool is_minmax_normalizer() const { return spec_.fusion_method_ == ObFusionMethod::MINMAX_NORMALIZER; }

private:
  int collect_all_data_batch();
  int get_next_batch_score_topk_query(const int64_t max_row_cnt);
  int get_next_batch_knn_only(const int64_t max_row_cnt);
  int get_next_batch_query_and_knn(const int64_t max_row_cnt);
  int fetch_next_child_batch(const int64_t batch_cnt,
                             const ObBatchRows *&child_brs,
                             bool &is_iter_end);
  void enter_output_knn_phase_if_needed(const bool is_iter_end);
  int get_query_score_expr(ObExpr *&query_score_expr,
                                bool &has_query_path,
                                int64_t &child_output_cnt);
  int prepare_batch_doc_indices(const int64_t batch_size);
  int classify_child_batch_rows(const ObBatchRows *child_brs,
                                ObExpr *query_score_expr,
                                const bool has_query_path,
                                const int64_t child_output_cnt,
                                bool &has_output);
  int push_scores_to_path_heaps(const ObBatchRows *child_brs,
                                const int64_t child_output_cnt);
  int prepare_output_knn_materialization();
  int compute_knn_only_topk();

  int init_path_heaps();
  int try_push_to_heaps(const ObBatchRows *child_brs, const int64_t start_row_store_idx);
  int add_top_k_info(ObTopKHeap *heap, double score, int64_t stored_row_idx, int64_t start_index, int64_t top_k_limit);

  int store_batch_rows(const ObBatchRows *child_brs);
  int store_batch_rows_knn_only(const ObBatchRows *child_brs);
  int get_min_max_score();

  int rescore();
  int rescore_by_rrf();
  int rescore_by_minmax();
  int rescore_by_weight_sum();

  int get_top_k_doc_indices();

  // Output
  int compute_fusion_topk();
  int emit_output_batch(const int64_t max_row_cnt);
  int get_store_row_batch(int64_t batch_size, const ObCompactRow **&stored_rows);
  int output_row_batch(const int64_t max_row_cnt, int64_t count);

  int init_constant_params();

  int try_push_to_heaps_rich_format(const ObBatchRows *child_brs,
                                    const ObIVector *score_vec,
                                    ObTopKHeap *heap,
                                    int64_t start_stored_row_idx,
                                    int64_t top_k_limit);
  int try_push_to_heaps_non_rich_format(const ObBatchRows *child_brs,
                                        const ObDatum *datums,
                                        ObTopKHeap *heap,
                                        int64_t start_stored_row_idx,
                                        int64_t top_k_limit);

  int output_row_rich_format(int64_t batch_size, const ObCompactRow **stored_rows);
  int output_row_non_rich_format(int64_t batch_size, const ObCompactRow **stored_rows);

  int build_fusion_docs_from_stored_order();
  static int normalize_rerank_info_to_utf8(common::ObIAllocator &allocator,
                                           const common::ObString &src,
                                           const common::ObCollationType src_coll,
                                           common::ObString &dst);
  static int rerank_json_index_to_int64(const common::ObJsonNode *node, int64_t &out);
  static int rerank_json_score_to_double(const common::ObJsonNode *node, double &out);
  int call_rerank();
  int sort_and_finalize();

  const ObHybridFusionSpec &spec_;
  bool is_data_ready_;
  int64_t top_k_limit_;
  int64_t size_;
  int64_t offset_;
  double min_score_;
  int64_t rank_constant_;
  int64_t output_idx_;
  int64_t path_count_;
  ObFusionIterExecMode fusion_iter_exec_mode_;
  int64_t search_index_;
  FullRecallPhase full_recall_phase_;
  ObRATempRowStore row_store_;
  ObRATempRowStore::RAReader row_store_reader_;
  common::ObSEArray<ObFusionDocInfo, 10> fusion_docs_;
  common::ObSEArray<ObPathStats, 10> path_stats_;
  common::ObSEArray<int64_t, 10> sorted_doc_indices_;
  common::ObSEArray<ObScoreEntryCompare, 4> comparers_;
  common::ObSEArray<double, 4> weights_;
  common::ObSEArray<int64_t, 4> path_top_k_limit_;
  common::ObSEArray<int64_t, 256> batch_doc_indices_;
  ObScoreEntryCompare fusion_score_comparer_;
  common::ObSEArray<ObTopKHeap*, 4> path_heaps_;
  hash::ObHashSet<int64_t> top_k_doc_indices_;
  common::ObSEArray<int64_t, 64> passthrough_doc_indices_;
  common::ObSEArray<const ObCompactRow*, 1024> stored_rows_buffer_;

  // AI rerank (when spec_.rerank_spec_.has_rerank_)
  int64_t rerank_window_size_;

  // SQL memory managements
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_BASIC_OB_HYBRID_FUSION_OP_H_ */