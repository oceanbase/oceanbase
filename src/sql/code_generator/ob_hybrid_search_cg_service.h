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

#ifndef DEV_SRC_SQL_CODE_GENERATOR_OB_HYBRID_SEARCH_CG_SERVICE_H_
#define DEV_SRC_SQL_CODE_GENERATOR_OB_HYBRID_SEARCH_CG_SERVICE_H_
#include "sql/hybrid_search/ob_hybrid_search_node.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/search/ob_das_boolean_query.h"
#include "sql/das/iter/ob_das_vec_index_scan_iter.h"
#include "sql/das/iter/ob_das_vec_index_driver_iter.h"

namespace oceanbase
{
namespace sql
{
class ObStaticEngineCG;
class ObTableScanCtDef;
class ObFullTextQueryNode;
class ObMatchQueryNode;
class ObDASMatchCtDef;
class ObMatchPhraseQueryNode;
class ObDASMatchPhraseCtDef;
class ObMultiMatchQueryNode;
class ObDASMultiMatchCtDef;
class ObQueryStringQueryNode;
class ObDASQueryStringCtDef;
class ObTextRetrievalIndexInfo;
class ObDASIRScanCtDef;

class ObHybridSearchCgService
{
public:
  ObHybridSearchCgService(ObStaticEngineCG &cg) : cg_(cg) { }
  virtual ~ObHybridSearchCgService() { }

  // main function to generate the hybrid search ctdef
  int generate_hybrid_search_ctdef(ObLogTableScan &op,
                                   ObTableScanCtDef &tsc_ctdef,
                                   ObDASBaseCtDef *&root_ctdef);

  int generate_ctdef(ObLogTableScan &op,
                     const ObScalarQueryNode* scalar_query_node,
                     ObDASScalarCtDef *&scalar_ctdef);

  int generate_ctdef(ObLogTableScan &op,
                     const ObBooleanQueryNode *boolean_query_node,
                     ObDASBooleanQueryCtDef *&boolean_query_ctdef);

  int generate_ctdef(ObLogTableScan &op,
                     const ObFusionNode *fusion_node,
                     ObDASFusionCtDef *&fusion_ctdef);

  int generate_ctdef(ObLogTableScan &op,
                     const ObVecSearchNode *vec_search_node,
                     ObDASVecIndexDriverCtDef *&vec_index_driver_ctdef);

  int generate_ctdef(ObLogTableScan &op,
                     const ObFullTextQueryNode *fulltext_query_node,
                     ObDASBaseCtDef *&fulltext_query_ctdef);

  // In the hybrid search scenario, tsc_ctdef.scan_ctdef_ is not used to perform scans, but it undertakes
  // functions such as partition routing and represents the primary partition corresponding to the DAS SCAN OP.
  // Therefore, it is still necessary to fill the required information.
  int mock_das_scan_ctdef(ObLogTableScan &op, ObDASScanCtDef &scan_ctdef);

  // generate the final lookup to project and possibly filter the result
  int generate_final_lookup_ctdef(ObLogTableScan &op,
                                  ObTableScanCtDef &tsc_ctdef,
                                  ObDASBaseCtDef &search_ctdef,
                                  ObDASBaseCtDef *&root_ctdef);
  int generate_main_scan_ctdef(ObLogTableScan &op, ObDASScanCtDef *&main_scan_ctdef);
  int extract_column_ids(const ObIArray<ObRawExpr *> &column_exprs, ObIArray<uint64_t> &column_ids);

private:
  class DASScalarCGParams
  {
  public:
    DASScalarCGParams(uint64_t data_table_id,
                      uint64_t index_table_id,
                      bool is_new_query_range,
                      bool is_rowkey_order_scan,
                      bool is_primary_table_scan,
                      share::schema::ObTableType table_type,
                      const ObIArray<ObRawExpr*> *access_exprs,
                      const ObIArray<uint64_t> *tsc_out_cols,
                      const ObIArray<ObRawExpr*> *rowkey_exprs,
                      const ObPreRangeGraph *pre_range_graph,
                      const ObQueryRange *pre_query_range,
                      bool has_pre_query_range = true,
                      bool is_get = false,
                      bool need_rowkey_order = true,
                      bool is_use_column_store = false,
                      const ObIArray<ObRawExpr*> *domain_id_expr = nullptr,
                      const ObIArray<uint64_t> *domain_id_col_ids = nullptr,
                      const ObIArray<ObRawExpr*> *pushdown_filters = nullptr,
                      const ObIArray<ObRawExpr*> *pushdown_aggr_exprs = nullptr,
                      const ObIArray<ObRawFilterMonotonicity> *filter_monotonicity = nullptr)
      : data_table_id_(data_table_id),
        index_table_id_(index_table_id),
        is_new_query_range_(is_new_query_range),
        is_rowkey_order_scan_(is_rowkey_order_scan),
        is_primary_table_scan_(is_primary_table_scan),
        table_type_(table_type),
        access_exprs_(access_exprs),
        tsc_out_cols_(tsc_out_cols),
        rowkey_exprs_(rowkey_exprs),
        pre_range_graph_(pre_range_graph),
        pre_query_range_(pre_query_range),
        has_pre_query_range_(has_pre_query_range),
        is_get_(is_get),
        need_rowkey_order_(need_rowkey_order),
        is_use_column_store_(is_use_column_store),
        domain_id_expr_(domain_id_expr),
        domain_id_col_ids_(domain_id_col_ids),
        pushdown_filters_(pushdown_filters),
        pushdown_aggr_exprs_(pushdown_aggr_exprs),
        filter_monotonicity_(filter_monotonicity)
    { }

    ~DASScalarCGParams() { reset(); }

    bool is_valid() const {
      return data_table_id_ != common::OB_INVALID_ID &&
             index_table_id_ != common::OB_INVALID_ID &&
             access_exprs_ != nullptr &&
             tsc_out_cols_ != nullptr &&
             rowkey_exprs_ != nullptr &&
             table_type_ != share::schema::MAX_TABLE_TYPE &&
             (!has_pre_query_range_ ||
                (is_new_query_range_ ? pre_range_graph_ != nullptr : pre_query_range_ != nullptr));
    }

    void init(uint64_t data_table_id,
              uint64_t index_table_id,
              bool is_new_query_range,
              bool is_rowkey_order_scan,
              bool is_primary_table_scan,
              share::schema::ObTableType table_type,
              const ObIArray<ObRawExpr*> *access_exprs,
              const ObIArray<uint64_t> *tsc_out_cols,
              const ObIArray<ObRawExpr*> *rowkey_exprs,
              const ObPreRangeGraph *pre_range_graph,
              const ObQueryRange *pre_query_range,
              bool has_pre_query_range = true,
              bool is_get = false,
              bool need_rowkey_order = true,
              bool is_use_column_store = false,
              const ObIArray<ObRawExpr*> *domain_id_expr = nullptr,
              const ObIArray<uint64_t> *domain_id_col_ids = nullptr,
              const ObIArray<ObRawExpr*> *pushdown_filters = nullptr,
              const ObIArray<ObRawExpr*> *pushdown_aggr_exprs = nullptr,
              const ObIArray<ObRawFilterMonotonicity> *filter_monotonicity = nullptr) {
      data_table_id_ = data_table_id;
      index_table_id_ = index_table_id;
      is_new_query_range_ = is_new_query_range;
      is_rowkey_order_scan_ = is_rowkey_order_scan;
      is_primary_table_scan_ = is_primary_table_scan;
      table_type_ = table_type;
      access_exprs_ = access_exprs;
      tsc_out_cols_ = tsc_out_cols;
      rowkey_exprs_ = rowkey_exprs;
      pre_range_graph_ = pre_range_graph;
      pre_query_range_ = pre_query_range;
      has_pre_query_range_ = has_pre_query_range;
      is_get_ = is_get;
      need_rowkey_order_ = need_rowkey_order;
      is_use_column_store_ = is_use_column_store;
      domain_id_expr_ = domain_id_expr;
      domain_id_col_ids_ = domain_id_col_ids;
      pushdown_filters_ = pushdown_filters;
      pushdown_aggr_exprs_ = pushdown_aggr_exprs;
      filter_monotonicity_ = filter_monotonicity;
    }

    void reset() {
      data_table_id_ = common::OB_INVALID_ID;
      index_table_id_ = common::OB_INVALID_ID;
      is_new_query_range_ = false;
      is_rowkey_order_scan_ = false;
      is_primary_table_scan_ = false;
      table_type_ = share::schema::MAX_TABLE_TYPE;
      access_exprs_ = nullptr;
      tsc_out_cols_ = nullptr;
      rowkey_exprs_ = nullptr;
      pre_range_graph_ = nullptr;
      pre_query_range_ = nullptr;
      has_pre_query_range_ = true;
      is_get_ = false;
      need_rowkey_order_ = true;
      is_use_column_store_ = false;
      domain_id_expr_ = nullptr;
      domain_id_col_ids_ = nullptr;
      pushdown_filters_ = nullptr;
      filter_monotonicity_ = nullptr;
    }

    #define DEF_VAL_ACCESSOR(type, name)                                \
      OB_INLINE type get_##name() const { return name##_; }
    #define DEF_BOOL_ACCESSOR(type, name)                               \
      OB_INLINE type name() const { return name##_; }
    #define DEFINE_GETTER(V, F)                                         \
      V(uint64_t,                             data_table_id)            \
      V(uint64_t,                             index_table_id)           \
      F(bool,                                 is_new_query_range)       \
      F(bool,                                 is_rowkey_order_scan)     \
      F(bool,                                 is_primary_table_scan)    \
      V(share::schema::ObTableType,           table_type)               \
      V(const ObIArray<ObRawExpr*> *,         access_exprs)             \
      V(const ObIArray<uint64_t> *,           tsc_out_cols)             \
      V(const ObIArray<ObRawExpr*> *,         rowkey_exprs)             \
      V(const ObPreRangeGraph *,              pre_range_graph)          \
      V(const ObQueryRange *,                 pre_query_range)          \
      F(bool,                                 has_pre_query_range)      \
      F(bool,                                 is_get)                   \
      F(bool,                                 need_rowkey_order)        \
      F(bool,                                 is_use_column_store)      \
      V(const ObIArray<ObRawExpr*> *,         domain_id_expr)           \
      V(const ObIArray<uint64_t> *,           domain_id_col_ids)        \
      V(const ObIArray<ObRawExpr*> *,         pushdown_filters)         \
      V(const ObIArray<ObRawExpr*> *,         pushdown_aggr_exprs)      \
      V(const ObIArray<ObRawFilterMonotonicity> *, filter_monotonicity)
    DEFINE_GETTER(DEF_VAL_ACCESSOR, DEF_BOOL_ACCESSOR)
    #undef DEF_VAL_ACCESSOR
    #undef DEF_BOOL_ACCESSOR
    #undef DEFINE_GETTER

  private:
    // necessary
    uint64_t data_table_id_;
    uint64_t index_table_id_;
    bool is_new_query_range_;                   // the query range is new type (ObPreRangeGraph) or old (ObQueryRange)
    bool is_rowkey_order_scan_;                 // the query range is rowkey order scan
    bool is_primary_table_scan_;                // scan the primary table
    share::schema::ObTableType table_type_;     // the table type
    const ObIArray<ObRawExpr*> *access_exprs_;  // exprs for accessing columns from the table
    const ObIArray<uint64_t> *tsc_out_cols_;    // col ids required for the table scan output
    const ObIArray<ObRawExpr*> *rowkey_exprs_;  // exprs representing the rowkey of the table
    const ObPreRangeGraph *pre_range_graph_;    // graph representation of query ranges (used if is_new_query_range_ is true)
    const ObQueryRange *pre_query_range_;       // standard representation of query ranges (used if is_new_query_range_ is false)

    // optional
    bool has_pre_query_range_;                    // has pre extracted query range for table scan
    bool is_get_;
    bool need_rowkey_order_;                        // the scan needs to preserve the order of rowkeys
    bool is_use_column_store_;                      // the scan should use column store storage format
    const ObIArray<ObRawExpr*> *domain_id_expr_;    // exprs for domain id
    const ObIArray<uint64_t> *domain_id_col_ids_;   // col ids corresponding to the domain id
    const ObIArray<ObRawExpr*> *pushdown_filters_;  // filters that can be pushed down to the storage layer
    const ObIArray<ObRawExpr*> *pushdown_aggr_exprs_; // aggr exprs pushed down to the storage layer
    const ObIArray<ObRawFilterMonotonicity> *filter_monotonicity_;  // monotonicity information for filters
  };

  int generate_hybrid_search_node_ctdef(ObLogTableScan &op,
                                        ObHybridSearchNodeBase *node,
                                        ObDASBaseCtDef *&root_ctdef);

  // used for scalar search ctdef generation
  int generate_scalar_scan_ctdef(const DASScalarCGParams &cg_params,
                                 ObDASScalarScanCtDef *&scan_ctdef);

  // used for vector aux table ctdef generation
  int generate_vec_aux_table_ctdef(ObLogTableScan &op,
                                   const ObVecSearchNode::VecIndexScanParams &vec_index_scan_params,
                                   ObDASScanCtDef *&scan_ctdef,
                                   bool is_get);

  int try_alloc_topk_collect_ctdef(ObDASFusionCtDef *fusion_ctdef);

  // Do we need to move text retrieval cg code to an independent class
  // since the only dependency interface here is generate_scalar_scan_ctdef()?
  int generate_fulltext_query_ctdef(ObLogTableScan &op,
                                    const ObMatchQueryNode *match_node,
                                    ObDASMatchCtDef *&match_query_ctdef);

  int generate_match_phrase_query_ctdef(ObLogTableScan &op,
                                        const ObMatchPhraseQueryNode *match_phrase_node,
                                        ObDASMatchPhraseCtDef *&match_phrase_query_ctdef);

  int generate_multi_match_query_ctdef(ObLogTableScan &op,
                                       const ObMultiMatchQueryNode *multi_match_node,
                                       ObDASMultiMatchCtDef *&multi_match_query_ctdef);

  int generate_query_string_query_ctdef(ObLogTableScan &op,
                                        const ObQueryStringQueryNode *query_string_node,
                                        ObDASQueryStringCtDef *&query_string_query_ctdef);

  int generate_text_ir_ctdef(ObLogTableScan &op,
                             const ObTextRetrievalIndexInfo &index_info,
                             const ObFullTextQueryNode &fulltext_node,
                             const bool is_topk_query,
                             const uint64_t inv_idx_tid,
                             const uint64_t doc_id_idx_tid,
                             ObDASIRScanCtDef *&ir_scan_ctdef);

  int generate_text_ir_sub_scan_ctdef(ObLogTableScan &op,
                                      const ObTextRetrievalIndexInfo &index_info,
                                      const ObTSCIRScanType ir_scan_type,
                                      const bool need_score,
                                      const bool need_pos_list,
                                      const uint64_t inv_idx_tid,
                                      const uint64_t doc_id_idx_tid,
                                      ObDASScalarScanCtDef *&scalar_scan_ctdef);

  int extract_text_ir_access_columns(const ObTextRetrievalIndexInfo &index_info,
                                     const ObTSCIRScanType ir_scan_type,
                                     const bool need_score,
                                     const bool need_pos_list,
                                     ObIArray<ObRawExpr *> &access_exprs,
                                     ObIArray<uint64_t> &output_cids,
                                     ObIArray<ObRawExpr *> &agg_exprs);

  int generate_text_ir_spec(const ObTextRetrievalIndexInfo &index_info,
                            const ObFullTextQueryNode &fulltext_node,
                            const bool is_topk_query,
                            const uint64_t inv_idx_tid,
                            ObDASIRScanCtDef &ir_scan_ctdef);

  int generate_block_max_scan_spec(const ObTextRetrievalIndexInfo &index_info,
                                   const uint64_t inv_idx_tid,
                                   ObDASIRScanCtDef &ir_scan_ctdef);

  int generate_avg_doc_len_est_spec(const ObTextRetrievalIndexInfo &index_info,
                                   const uint64_t inv_idx_tid,
                                   ObDASIRScanCtDef &ir_scan_ctdef);

  int append_block_max_scan_agg_column(const int64_t column_id,
                                       const ObTableSchema &table_schema,
                                       const ObSkipIndexColType skip_index_type,
                                       const ObIArray<ObColDesc> &col_descs,
                                       const ObIArray<uint64_t> &access_column_ids,
                                       ObIArray<int32_t> &block_max_scan_col_store_idxes,
                                       ObIArray<ObSkipIndexColType> &block_max_scan_col_types,
                                       ObIArray<int32_t> &block_max_scan_col_proj);

  int check_skip_index_validity(const ObTextRetrievalIndexInfo &index_info,
                                const uint64_t inv_idx_tid,
                                bool &is_valid) const;
  int check_column_ids_accessible(
    uint64_t ref_table_id, const ObSqlSchemaGuard &schema_guard, const ObIArray<uint64_t> &column_ids);

private:
  DISALLOW_COPY_AND_ASSIGN(ObHybridSearchCgService);
  ObStaticEngineCG &cg_;
};

} // namespace sql
} // namespace oceanbase
#endif // DEV_SRC_SQL_CODE_GENERATOR_OB_HYBRID_SEARCH_CG_SERVICE_H_
