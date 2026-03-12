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
};

class ObHybridFusionOp : public ObOperator
{
public:
  ObHybridFusionOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input) :
    ObOperator(exec_ctx, spec, input), spec_(static_cast<const ObHybridFusionSpec&>(spec)),
    is_data_ready_(false), top_k_limit_(10), size_(10), offset_(0), min_score_(0.0),
    output_idx_(0), path_count_(0), profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
    sql_mem_processor_(profile_, op_monitor_info_) {}

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

  int init_path_heaps();
  int try_push_to_heaps(const ObBatchRows *child_brs, const int64_t start_row_store_idx);
  int add_top_k_info(ObTopKHeap *heap, double score, int64_t stored_row_idx, int64_t start_index, int64_t top_k_limit);

  int store_batch_rows(const ObBatchRows *child_brs);
  int get_min_max_score();

  int rescore();
  int rescore_by_rrf();
  int rescore_by_minmax();
  int rescore_by_weight_sum();

  int get_top_k_doc_indices();

  // Output
  int compute_fusion_topk();
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

  const ObHybridFusionSpec &spec_;
  bool is_data_ready_;
  int64_t top_k_limit_;
  int64_t size_;
  int64_t offset_;
  double min_score_;
  int64_t rank_constant_;
  int64_t output_idx_;
  int64_t path_count_;
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
  common::ObSEArray<const ObCompactRow*, 1024> stored_rows_buffer_;

  // SQL memory managements
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_BASIC_OB_HYBRID_FUSION_OP_H_ */