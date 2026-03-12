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

#ifndef _OB_HYBRID_SEARCH_NODE_H
#define _OB_HYBRID_SEARCH_NODE_H 1
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/hybrid_search/ob_hybrid_search_dsl_resolver.h"
#include "sql/hybrid_search/ob_fulltext_search_query.h"

namespace oceanbase
{
namespace sql
{
class ObFullTextQueryNode;

class ObHybridSearchNodeBase : public ObIndexMergeNode
{
public:
  ObHybridSearchNodeBase(common::ObIAllocator &allocator) : ObIndexMergeNode(allocator), boost_(nullptr), is_scoring_(false),
    is_top_level_scoring_(false), index_name_() {}
  virtual ~ObHybridSearchNodeBase() {}
  virtual inline bool is_hybrid_scalar_node() const { return false; }
  virtual inline bool is_hybrid_scalar_node_with_index() const { return false; }
  virtual inline bool is_hybrid_search_node() const { return true; }
  virtual int generate_access_exprs(const TableItem &table_item, ObOptimizerContext &opt_ctx);
  virtual int collect_exprs(ObIArray<ObRawExpr*> &query_exprs, ObIArray<ObRawExpr*> &score_exprs) const;
  virtual int explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count) = 0;
  virtual int get_all_index_ids(ObIArray<uint64_t> &index_ids) const;
  virtual int extract_filter(ObRawExprFactory &expr_factory, ObRawExpr *&extracted_expr) {
    UNUSED(expr_factory);
    extracted_expr = nullptr;
    return OB_NOT_SUPPORTED;
  }
  int get_all_scan_node(ObIArray<ObIndexMergeNode*> &scan_nodes);
  int collect_hybrid_search_exprs(ObIArray<ObRawExpr*> &query_exprs, ObIArray<ObRawExpr*> &score_exprs) const;
  int collect_vec_infos(ObIArray<ObVecIndexInfo*> &vec_infos);
  int get_index_name(ObIArray<ObString> &index_names);
  int print_blank_space(char *buf, int64_t buf_len, int64_t &pos, int n);
  ObConstRawExpr *boost_;
  bool is_scoring_;
  bool is_top_level_scoring_;
  ObString index_name_; // plan info explain
};

class ObBooleanQueryNode : public ObHybridSearchNodeBase
{
public:
  ObBooleanQueryNode(common::ObIAllocator &allocator) : ObHybridSearchNodeBase(allocator), must_nodes_(allocator), should_nodes_(allocator),
    filter_nodes_(allocator), must_not_nodes_(allocator), min_should_match_(0), origin_expr_(nullptr)
    { node_type_ = INDEX_MERGE_HYBRID_BOOLEAN_QUERY; }
  virtual ~ObBooleanQueryNode() {}
  virtual int explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count);
  virtual int extract_filter(ObRawExprFactory &expr_factory, ObRawExpr *&extracted_expr) override;
public:
  ObSqlArray<ObIndexMergeNode*> must_nodes_;
  ObSqlArray<ObIndexMergeNode*> should_nodes_;
  ObSqlArray<ObIndexMergeNode*> filter_nodes_;
  ObSqlArray<ObIndexMergeNode*> must_not_nodes_;
  double min_should_match_;
  ObRawExpr *origin_expr_; // for search index
};

class ObFusionNode : public ObHybridSearchNodeBase
{
public:
  ObFusionNode(common::ObIAllocator &allocator) : ObHybridSearchNodeBase(allocator), method_(ObFusionMethod::WEIGHT_SUM),
    from_(nullptr), size_(nullptr), min_score_(nullptr), window_size_(nullptr), rank_const_(nullptr),
    rowkey_cols_(allocator), score_cols_(allocator), weight_cols_(allocator), path_top_k_limit_(allocator),
    contains_vec_node_(false), search_index_(-1), has_search_subquery_(false), has_vector_subquery_(false),
    is_top_k_query_(true), has_hybrid_fusion_op_(false)
    { node_type_ = INDEX_MERGE_HYBRID_FUSION_SEARCH; }
  virtual ~ObFusionNode() {}
  virtual inline bool contains_vector_node() const { return contains_vec_node_; }
  virtual int explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count);
  ObRawExpr *get_fusion_score_expr() const {return score_cols_.empty() ? nullptr : score_cols_.at(score_cols_.count() - 1);}

  ObFusionMethod method_;
  ObRawExpr *from_;
  ObRawExpr *size_;
  ObRawExpr *min_score_;
  ObRawExpr *window_size_;
  ObRawExpr *rank_const_;
  ObSqlArray<ObRawExpr*> rowkey_cols_;
  ObSqlArray<ObRawExpr*> score_cols_;
  ObSqlArray<ObRawExpr*> weight_cols_;
  ObSqlArray<ObRawExpr*> path_top_k_limit_;

  bool contains_vec_node_;
  int64_t search_index_;
  bool has_search_subquery_;
  bool has_vector_subquery_;
  bool is_top_k_query_;
  bool has_hybrid_fusion_op_;
};

class ObScalarQueryNode : public ObHybridSearchNodeBase
{
public:
  ObScalarQueryNode(common::ObIAllocator &allocator) : ObHybridSearchNodeBase(allocator),
                        need_rowkey_order_(true),
                        pri_table_ap_(nullptr),
                        pri_table_query_params_(allocator),
                        index_table_query_params_(allocator),
                        index_table_rowkey_exprs_(allocator),
                        has_valid_index_(false),
                        is_extracted_(false)
  { node_type_ = INDEX_MERGE_HYBRID_SCALAR_QUERY; }
  virtual ~ObScalarQueryNode() {}
  virtual inline bool is_hybrid_scalar_node() const { return true; }
  virtual inline bool is_hybrid_scalar_node_with_index() const { return has_index(); }
  virtual int explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count);
  virtual int extract_filter(ObRawExprFactory &expr_factory, ObRawExpr *&extracted_expr) override;
  inline bool has_index() const { return has_valid_index_; }

public:
  struct ScalarQueryParams
  {
  public:
    ScalarQueryParams(common::ObIAllocator &allocator) : access_exprs_(allocator), output_col_ids_(allocator),
      pushdown_filters_(allocator), filter_monotonicity_(allocator)
    {}
    virtual ~ScalarQueryParams() {}

    ObSqlArray<ObRawExpr*> access_exprs_;
    ObSqlArray<uint64_t> output_col_ids_;
    ObSqlArray<ObRawExpr*> pushdown_filters_;
    ObSqlArray<ObRawFilterMonotonicity> filter_monotonicity_;
  };

  bool need_rowkey_order_;
  // In hybrid search, scalar queries may not preserve primary key order.
  // Here we provide an optimization: use the index column query conditions as filters
  // and perform a sequential scan on the primary table, so that the results can be streamed in primary key order.
  AccessPath *pri_table_ap_;
  ScalarQueryParams pri_table_query_params_;
  ScalarQueryParams index_table_query_params_;
  // For search index, rowkey exprs are not the same as primary table, store it separately
  ObSqlArray<ObRawExpr*> index_table_rowkey_exprs_;
  bool has_valid_index_;
  bool is_extracted_;  // For vec search: all filter exprs are extracted to generate a new scalar query node
};

class ObVecSearchNode : public ObHybridSearchNodeBase
{
public:
  ObVecSearchNode(common::ObIAllocator &allocator) : ObHybridSearchNodeBase(allocator),
                      filter_nodes_(allocator),
                      extracted_scalar_node_(nullptr),
                      main_index_table_id_(OB_INVALID_ID),
                      vec_info_(allocator),
                      field_(nullptr),
                      table_schema_(nullptr),
                      search_option_(nullptr) {
                        node_type_ = INDEX_MERGE_HYBRID_KNN;
                      }
  virtual ~ObVecSearchNode() {};

  int init_vec_info(ObSqlSchemaGuard *schema_guard);
  virtual int explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count);
public:
  struct VecIndexScanParams
  {
  public:
    VecIndexScanParams() : access_exprs_(), table_id_(OB_INVALID_ID), scan_type_(ObTSCIRScanType::OB_IR_INV_IDX_SCAN) {}
    virtual ~VecIndexScanParams() {}

    common::ObSEArray<ObRawExpr*, 2, common::ModulePageAllocator, true> access_exprs_;
    ObTableID table_id_;
    ObTSCIRScanType scan_type_;
  };

  const ObVectorIndexQueryParam &get_query_param() const { return vec_info_.query_param_; }
  const ObVectorIndexParam &get_vector_index_param() const { return vec_info_.vector_index_param_; }
  ObVectorIndexAlgorithmType get_algorithm_type() const { return vec_info_.get_vec_algorithm_type(); }
  ObVecIndexType get_vec_type() const { return vec_info_.vec_type_; }
  int64_t get_row_count() const { return vec_info_.row_count_; }
  ObTableID get_data_table_id() const { return vec_info_.main_table_tid_; }
  ObRawExpr* get_target_vec_column() const { return static_cast<ObRawExpr*>(vec_info_.target_vec_column_); }

  OrderItem get_sort_key() const { return vec_info_.sort_key_; }
  ObRawExpr *get_topk_limit_expr() const { return vec_info_.topk_limit_expr_; }
  ObRawExpr *get_topk_offset_expr() const { return vec_info_.topk_offset_expr_; }

  int get_delta_buf_table_params(VecIndexScanParams &params) const;
  int get_index_id_table_params(VecIndexScanParams &params) const;
  int get_snapshot_table_params(VecIndexScanParams &params) const;
  int get_com_aux_vec_table_params(VecIndexScanParams &params) const;
  inline bool need_extract_filter() const
  {
    return search_option_ == nullptr
        || search_option_->filter_mode_ == ObKnnFilterMode::POST_FILTER
        || search_option_->filter_mode_ == ObKnnFilterMode::INVALID_KNN_FILTER_MODE;
  }

  ObSqlArray<ObIndexMergeNode*> filter_nodes_;
  ObScalarQueryNode *extracted_scalar_node_;  // for iterative filter
  ObTableID main_index_table_id_;
  ObVecIndexInfo vec_info_;
  ObColumnRefRawExpr *field_;
  const ObTableSchema *table_schema_;
  const ObDSLKnnQuery::SearchOption *search_option_;
};

class ObHybridSearchGenerator {
public:
  explicit ObHybridSearchGenerator(common::ObIAllocator *alloc, ObLogPlan *plan, ObSqlSchemaGuard *schema_guard,
    const ObTableSchema *table_schema, const ObIArray<uint64_t> &valid_index_ids,
    const ObIArray<ObSEArray<uint64_t, 4>> &valid_index_cols,
    const ObIArray<bool> &index_can_ignore_prefix, const TableItem *table_item)
    : allocator_(alloc), plan_(plan), schema_guard_(schema_guard), table_schema_(table_schema),
      valid_index_ids_(valid_index_ids), valid_index_cols_(valid_index_cols),
      index_can_ignore_prefix_(index_can_ignore_prefix),
      fusion_node_(nullptr), table_item_(table_item) {}
  virtual ~ObHybridSearchGenerator() {}
  int generate(const ObDSLQueryInfo *dsl_query, ObIndexMergeNode *&hybrid_search_tree);
  int deal_table_scan_filters(ObIndexMergeNode *hybrid_search_tree,
                              ObIArray<ObRawExpr*> &table_filters,
                              bool &ingore_normal_access_path);
  int move_bool_nodes_to_children(ObIndexMergeNode* node);
private:
  int generate_node(const ObDSLQuery *query, ObIndexMergeNode *&node);
  int generate_boolean_node(const ObDSLBoolQuery *dsl_query, ObBooleanQueryNode *&bool_node);
  int generate_scalar_node(ObRawExpr *filter, ObScalarQueryNode *&scalar_node);
  int generate_fulltext_node(const ObDSLFullTextQuery *fulltext_query, ObFullTextQueryNode *&fulltext_node);
  int generate_knn_node(const ObDSLKnnQuery *knn_query, ObVecSearchNode *&knn_node);
  int try_merge_nodes(const ObDSLQuery *query, ObIArray<ObIndexMergeNode*> &nodes, bool &merge_happened);
  int check_filter_has_index(ObRawExpr *filter, bool &has_index, uint64_t &target_index_id);
  int check_filter_has_search_index(ObRawExpr *filter, bool &has_search_index);
  int init_fusion_node(const ObDSLQueryInfo *query_info, ObFusionNode *fusion_node);
  int get_vector_index_tid_from_expr(ObColumnRefRawExpr *field, uint64_t& vec_index_tid);
  bool is_search_subquery(ObIndexMergeNode *node) const;
  bool is_knn_subquery(ObIndexMergeNode *node) const;
  int collect_child_boost_and_update_flags(ObFusionNode *fusion_node,
                                           ObIndexMergeNode *sub_node,
                                           ObConstRawExpr *one_boost_expr,
                                           int64_t child_idx);
  int deal_child_node(ObIndexMergeNode *node, bool &prune_happened, double &sel);
  int recurse_count_index_nodes(const ObIndexMergeNode *node, int64_t &index_scan_count,
                                bool &ingore_normal_access_path) const;
  int split_domain_contains_node(const ObDSLScalarQuery *scalar_query,
                                 ObIndexMergeNode *&split_node);
  int split_json_contains(const ObDSLScalarQuery *scalar_query,
                          ObRawExpr *filter_expr,
                          ObIndexMergeNode *&split_node);
  int split_array_contains_all(const ObDSLScalarQuery *scalar_query,
                               ObRawExpr *filter_expr,
                               ObIndexMergeNode *&split_node);
  static bool in_merge_expr_whitelist(const ObRawExpr *expr);
private :
  common::ObIAllocator *allocator_;
  ObLogPlan *plan_;
  ObSqlSchemaGuard *schema_guard_;
  const ObTableSchema *table_schema_;
  const ObIArray<uint64_t> &valid_index_ids_;
  const ObIArray<ObSEArray<uint64_t, 4>> &valid_index_cols_;
  const ObIArray<bool> &index_can_ignore_prefix_;
  ObFusionNode *fusion_node_;
  const TableItem *table_item_;
  static const double MAX_INTERSECTION_MERGE_SEL;
  static const double MAX_SEL_GAP_RATIO;
};

}
}

#endif
