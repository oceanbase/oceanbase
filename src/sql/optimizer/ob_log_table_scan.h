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

#ifndef OCEANBASE_SQL_OB_LOG_TABLE_SCAN_H
#define OCEANBASE_SQL_OB_LOG_TABLE_SCAN_H 1
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/resolver/dml/ob_sql_hint.h"
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_query_range.h"
#include "sql/optimizer/ob_log_set.h"

namespace oceanbase
{
namespace sql
{
class Path;

struct ObTextRetrievalInfo
{
  ObTextRetrievalInfo()
  : match_expr_(NULL),
    pushdown_match_filter_(NULL),
    sort_key_(),
    topk_limit_expr_(NULL),
    topk_offset_expr_(NULL),
    with_ties_(false),
    need_calc_relevance_(true),
    inv_idx_tid_(OB_INVALID_ID),
    fwd_idx_tid_(OB_INVALID_ID),
    doc_id_idx_tid_(OB_INVALID_ID),
    token_column_(NULL),
    token_cnt_column_(NULL),
    doc_id_column_(NULL),
    doc_length_column_(NULL),
    related_doc_cnt_(NULL),
    total_doc_cnt_(NULL),
    doc_token_cnt_(NULL),
    relevance_expr_(NULL)
  { }
  ~ObTextRetrievalInfo() {}

  TO_STRING_KV(K_(match_expr), K_(pushdown_match_filter), K_(sort_key), K_(topk_limit_expr),
               K_(topk_offset_expr), K_(with_ties), K_(need_calc_relevance), K_(inv_idx_tid),
               K_(fwd_idx_tid), K_(doc_id_idx_tid));

  bool need_sort() const { return sort_key_.expr_ != nullptr; }

  ObMatchFunRawExpr *match_expr_;
  ObRawExpr *pushdown_match_filter_;
  OrderItem sort_key_;  // for pushdown topk, only support match expr as sort expr
  ObRawExpr *topk_limit_expr_;
  ObRawExpr *topk_offset_expr_;
  bool with_ties_;
  bool need_calc_relevance_;  // match expr just for retireval (accurate score is not required)
  uint64_t inv_idx_tid_;  // choosed aux inverted index table id (word-doc)
  uint64_t fwd_idx_tid_;  // choosed aux forward index table id (doc-word)
  uint64_t doc_id_idx_tid_; // choosed aux doc_id index table id (doc-rowkey)
  // the following exprs are used for intermediate calculation of relevance score
  ObColumnRefRawExpr *token_column_;
  ObColumnRefRawExpr *token_cnt_column_;
  ObColumnRefRawExpr *doc_id_column_;
  ObColumnRefRawExpr *doc_length_column_;
  ObAggFunRawExpr *related_doc_cnt_;  // count(token_cnt_column)
  ObAggFunRawExpr *total_doc_cnt_;  // count(doc_id_column)
  ObAggFunRawExpr *doc_token_cnt_;  // sum(token_cnt_column)
  ObRawExpr *relevance_expr_; // BM25
};

struct ObRawFilterMonotonicity
{
  ObRawFilterMonotonicity() : filter_expr_(NULL),
                              col_expr_(NULL),
                              mono_(PushdownFilterMonotonicity::MON_NON),
                              assist_exprs_() {}



  ObRawExpr *filter_expr_;
  ObColumnRefRawExpr *col_expr_;
  PushdownFilterMonotonicity mono_;
  common::ObFixedArray<ObRawExpr *, common::ObIAllocator> assist_exprs_;
  TO_STRING_KV(K_(filter_expr),
               K_(col_expr),
               K_(mono),
               K_(assist_exprs));
};

class ObLogTableScan : public ObLogicalOperator
{
public:
  ObLogTableScan(ObLogPlan &plan)
      : ObLogicalOperator(plan),
        table_id_(common::OB_INVALID_ID ),
        ref_table_id_(common::OB_INVALID_ID ),
        index_table_id_(common::OB_INVALID_ID ),
        session_id_(0),
        advisor_table_id_(OB_INVALID_ID),
        is_index_global_(false),
        is_spatial_index_(false),
        is_multivalue_index_(false),
        use_das_(false),
        index_back_(false),
        is_multi_part_table_scan_(false),
        table_name_(),
        index_name_(),
        scan_direction_(default_asc_direction()),
        for_update_(false),
        for_update_wait_us_(-1), /* default infinite */
        pre_query_range_(NULL),
        part_ids_(NULL),
        filter_before_index_back_(),
        table_partition_info_(NULL),
        ranges_(),
        ss_ranges_(),
        is_skip_scan_(),
        limit_count_expr_(NULL),
        limit_offset_expr_(NULL),
        sample_info_(),
        est_cost_info_(NULL),
        table_opt_info_(NULL),
        est_records_(),
        part_expr_(NULL),
        subpart_expr_(NULL),
        gi_charged_(false),
        gi_alloc_post_state_forbidden_(false),
        diverse_path_count_(0),
        fq_expr_(NULL),
        fq_type_(TableItem::NOT_USING),
        fq_read_tx_uncommitted_(false),
        bf_info_(),
        part_join_filter_allocated_(false),
        group_id_expr_(nullptr),
        use_batch_(false),
        access_path_(NULL),
        tablet_id_expr_(NULL),
        tablet_id_type_(0),
        calc_part_id_expr_(NULL),
        trans_info_expr_(NULL),
        identify_seq_expr_(nullptr),
        global_index_back_table_partition_info_(NULL),
        has_index_scan_filter_(false),
        has_index_lookup_filter_(false),
        table_type_(share::schema::MAX_TABLE_TYPE),
        use_column_store_(false),
        doc_id_table_id_(common::OB_INVALID_ID),
        text_retrieval_info_(),
        das_keep_ordering_(false),
        filter_monotonicity_()
  {
  }

  virtual ~ObLogTableScan() {}

  const char *get_name() const;

  // not used at the moment
  TO_STRING_KV(K_(table_id), K_(index_table_id), K_(table_name), K_(index_name));
  /**
   *  Get table id
   */
  inline uint64_t get_table_id() const
  { return table_id_; }

  /**
   *  Get ref table id
   */
  inline uint64_t get_ref_table_id() const
  { return ref_table_id_; }

  inline uint64_t get_real_ref_table_id() const
  {
    //for the local index lookup,
    //need to use the local index id as the table_id for partition calculation
    //for oracle mapping virtual table
    //get the real table location info use the real table id
    return share::is_oracle_mapping_real_virtual_table(ref_table_id_) ?
      ObSchemaUtils::get_real_table_mappings_tid(ref_table_id_) : ref_table_id_;
  }

  /*
   * get is global index
   */
  inline bool get_is_index_global() const
  {  return is_index_global_; }

  inline void set_use_das(bool use_das)
  { use_das_ = use_das; }

  inline bool use_das() const
  { return use_das_; }

  inline void set_use_column_store(bool use_column_store)
  { use_column_store_ = use_column_store; }

  inline bool use_column_store() const
  { return use_column_store_; }

  /**
   *  Get index table id
   */
  inline uint64_t get_index_table_id() const
  { return index_table_id_; }

  inline uint64_t get_real_index_table_id() const
  {
    //for the local index lookup,
    //need to use the local index id as the table_id for partition calculation
    //for oracle mapping virtual table
    //get the real table location info use the real table id
    return share::is_oracle_mapping_real_virtual_table(index_table_id_) ?
      ObSchemaUtils::get_real_table_mappings_tid(index_table_id_) : index_table_id_;
  }

  inline uint64_t get_advisor_table_id() const
  {
    return advisor_table_id_;
  }

  inline void set_advisor_table_id(uint64_t advise_table_id)
  {
    advisor_table_id_ = advise_table_id;
  }

  bool is_duplicate_table();

  /**
   *  Get pre query range
   */
  inline const ObQueryRange *get_pre_query_range() const
  { return pre_query_range_; }

  /**
   *  Get range columns
   */
  inline const common::ObIArray<ColumnItem> &get_range_columns() const
  { return range_columns_; }

  /**
   *  Set table id
   */
  inline void set_table_id(uint64_t table_id)
  { table_id_ = table_id; }

  /**
   *  Set ref table id
   */
  void set_ref_table_id(uint64_t ref_table_id);

  /**
   *  Set index table id
   */
  inline void set_index_table_id(uint64_t index_table_id)
  { index_table_id_ = index_table_id; }

  /*
   * set is global index id
   */
  inline void set_is_index_global(bool is_index_global)
  { is_index_global_ = is_index_global; }

  /*
   * set is spatial index
   */
  inline void set_is_spatial_index(bool is_spatial_index)
  { is_spatial_index_ = is_spatial_index; }

  inline bool get_is_spatial_index() const
  { return is_spatial_index_; }

  /*
   * set is multivalue index
   */
  inline void set_is_multivalue_index(bool is_multivalue_index)
  { is_multivalue_index_ = is_multivalue_index; }

  inline bool get_is_multivalue_index() const
  { return is_multivalue_index_; }

  /**
   *  Set scan direction
   */
  inline void set_scan_direction(ObOrderDirection direction)
  {
    scan_direction_ = direction;
    common::ObIArray<OrderItem> &op_ordering = get_op_ordering();
    for (int64_t i = 0; i < op_ordering.count(); ++i) {
      op_ordering.at(i).order_type_ = scan_direction_;
    }
  }

  /**
   *  Set pre query range
   */
  inline void set_pre_query_range(const ObQueryRange *query_range)
  { pre_query_range_ = query_range; }

  /**
   *  Set range columns
   */
  int set_range_columns(const common::ObIArray<ColumnItem> &range_columns);
  int add_idx_column_id(const uint64_t column_id)
  { return idx_columns_.push_back(column_id); }

  const common::ObIArray<uint64_t> &get_idx_columns() const
  { return idx_columns_; }

  void set_est_cost_info(ObCostTableScanInfo *param)
  { est_cost_info_ = param; }

  const ObCostTableScanInfo *get_est_cost_info() const
  { return est_cost_info_; }

  ObCostTableScanInfo *get_est_cost_info()
  { return est_cost_info_; }

  int set_update_info();

  void set_part_ids(const common::ObIArray<int64_t> *part_ids) { part_ids_ = part_ids; }
  const common::ObIArray<int64_t> *get_part_ids() { return part_ids_; }

  void set_part_expr(ObRawExpr *part_expr) { part_expr_ = part_expr; }
  ObRawExpr *get_part_expr() const { return part_expr_; }
  void set_subpart_expr(ObRawExpr *subpart_expr) { subpart_expr_ = subpart_expr; }
  ObRawExpr *get_subpart_expr() const { return subpart_expr_; }

  //should check index back after project pruning.Get final index back.
  virtual int index_back_check();

  /**
   *  Get access expressions
   */
  inline const common::ObIArray<ObRawExpr *> &get_access_exprs() const
  { return access_exprs_; }

  /**
   *  Get access expressions
   */
  inline common::ObIArray<ObRawExpr *> &get_access_exprs()
  { return access_exprs_; }

// removal it in cg layer, up to opt layer.
  inline const common::ObIArray<uint64_t> &get_ddl_output_column_ids() const
  { return ddl_output_column_ids_; }

  inline common::ObIArray<ObRawExpr *> &get_ext_file_column_exprs()
  { return ext_file_column_exprs_; }

  inline common::ObIArray<ObRawExpr *> &get_ext_column_convert_exprs()
  { return ext_column_convert_exprs_; }

  ObRawExpr* get_real_expr(const ObRawExpr *col) const;
  /**
   *  Get pushdown aggr expressions
   */
  inline const common::ObIArray<ObAggFunRawExpr *> &get_pushdown_aggr_exprs() const
  { return pushdown_aggr_exprs_; }

  /**
   *  Get pushdown aggr expressions
   */
  inline common::ObIArray<ObAggFunRawExpr *> &get_pushdown_aggr_exprs()
  { return pushdown_aggr_exprs_; }

  inline common::ObIArray<ObRawExpr *> &get_pushdown_groupby_columns() { return pushdown_groupby_columns_; }

  inline const common::ObIArray<ObRawExpr *> &get_pushdown_groupby_columns() const { return pushdown_groupby_columns_; }
  inline const common::ObIArray<ObRawExpr *> &get_domain_exprs() const { return domain_exprs_; }

  /**
   * Generate the filtering expressions
   */
  int gen_filters();

  /**
   *  Allocate granule iterator
   * */
  virtual int allocate_granule_pre(AllocGIContext &ctx) override;
  /**
   *  Allocate granule iterator
   * */
  virtual int allocate_granule_post(AllocGIContext &ctx) override;


  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;

  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int allocate_expr_post(ObAllocExprContext &ctx);
  virtual int check_output_dependance(common::ObIArray<ObRawExpr *> &child_output, PPDeps &deps);

  /**
   *  Generate hash value for the operator using given seed
   */
  virtual uint64_t hash(uint64_t seed) const override;

  /**
   *  Get table name
   */
  inline common::ObString &get_table_name() { return table_name_; }
  inline const common::ObString &get_table_name() const { return table_name_; }
  inline void set_table_name(const common::ObString &table_name) { table_name_ = table_name; }

  /**
   *  Get index name
   */
  inline const  common::ObString &get_index_name() const { return index_name_; }

  inline common::ObString &get_index_name() { return index_name_; }

  inline void set_index_name(common::ObString &index_name)
  { index_name_= index_name; }

  inline ObTablePartitionInfo *get_table_partition_info() { return table_partition_info_; }
  inline const ObTablePartitionInfo *get_table_partition_info() const { return table_partition_info_; }
  inline void set_table_partition_info(ObTablePartitionInfo *table_partition_info) { table_partition_info_ = table_partition_info; }

  bool is_index_scan() const { return ref_table_id_ != index_table_id_; }
  bool is_table_whole_range_scan() const { return !is_index_scan() && (NULL == pre_query_range_ ||
                                                  (1 == ranges_.count() && ranges_.at(0).is_whole_range())); }
  void set_skip_scan(bool is_skip_scan) { is_skip_scan_ = is_skip_scan; }
  bool is_skip_scan() const { return is_skip_scan_; }
  virtual bool is_table_scan() const override { return true; }
  bool is_whole_range_scan() const {return NULL == pre_query_range_
                                            || (1 == ranges_.count() && ranges_.at(0).is_whole_range()); }
  ObOrderDirection get_scan_direction() const { return scan_direction_; }
  void set_index_back(bool index_back) { index_back_ = index_back; }
  bool get_index_back() const { return index_back_; }
  void set_is_multi_part_table_scan(bool multi_part_tsc)
  { is_multi_part_table_scan_ = multi_part_tsc; }
  bool get_is_multi_part_table_scan() { return is_multi_part_table_scan_; }
  int set_query_ranges(ObIArray<ObNewRange> &ranges, ObIArray<ObNewRange> &ss_ranges);
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
  inline common::ObIArray<bool> &get_filter_before_index_flags() { return filter_before_index_back_; }
  inline const common::ObIArray<bool> &get_filter_before_index_flags() const { return filter_before_index_back_; }
  inline ObRawExpr *get_limit_expr() { return limit_count_expr_; }
  inline ObRawExpr *get_offset_expr() { return limit_offset_expr_; }
  int set_limit_offset(ObRawExpr *limit, ObRawExpr *offset);
  inline int64_t get_table_row_count() const
  { return est_cost_info_ == NULL || est_cost_info_->table_meta_info_ == NULL ? 0.0 : est_cost_info_->table_meta_info_->table_row_count_; }
  inline double get_output_row_count() const { return est_cost_info_ == NULL ? 0.0 : est_cost_info_->output_row_count_; }
  inline double get_phy_query_range_row_count() const { return est_cost_info_ == NULL ? 0.0 : est_cost_info_->phy_query_range_row_count_; }
  inline double get_logical_query_range_row_count() const { return est_cost_info_ == NULL ? 0.0 : est_cost_info_->logical_query_range_row_count_; }
  inline void set_index_back_row_count(double index_back_row_count) { if (est_cost_info_ != NULL) est_cost_info_->index_back_row_count_ = index_back_row_count; }
  inline double get_index_back_row_count() const { return est_cost_info_ == NULL ? 0.0 : est_cost_info_->index_back_row_count_; }
  int is_top_table_scan(bool &is_top_table_scan)
  {
    int ret = common::OB_SUCCESS;
    is_top_table_scan = false;
    if (NULL == get_parent()) {
      is_top_table_scan = true;
    } else if (log_op_def::LOG_EXCHANGE == get_parent()->get_type()
               && OB_ISNULL(get_parent()->get_parent())) {
      ret = common::OB_ERR_UNEXPECTED;
    } else if (log_op_def::LOG_EXCHANGE == get_parent()->get_type()
               && NULL == get_parent()->get_parent()->get_parent()) {
      is_top_table_scan = true;
    } else { /* Do nothing */ }
    return ret;
  }
  int get_path_ordering(common::ObIArray<ObRawExpr *> &order_exprs);

  inline void set_table_opt_info(BaseTableOptInfo *table_opt_info)
  { table_opt_info_ = table_opt_info; }

  int set_est_row_count_record(common::ObIArray<common::ObEstRowCountRecord> &est_records)
  { return est_records_.assign(est_records); }

  int set_query_range_exprs(const common::ObIArray<ObRawExpr *> &range_exprs)
  { return range_conds_.assign(range_exprs); }

  ObPxBFStaticInfo &get_join_filter_info() { return bf_info_; }

  void set_join_filter_info(ObPxBFStaticInfo &bf_info) { bf_info_ = bf_info; }

  inline BaseTableOptInfo* get_table_opt_info() { return table_opt_info_; }

  ObPxRFStaticInfo &get_px_rf_info() { return px_rf_info_; }
  int set_px_rf_info(ObPxRFStaticInfo &px_rf_info) { return px_rf_info_.assign(px_rf_info); }

  inline const common::ObIArray<common::ObEstRowCountRecord> &get_est_row_count_record() const
  { return est_records_; }

  inline SampleInfo &get_sample_info() { return sample_info_; }
  inline const SampleInfo &get_sample_info() const { return sample_info_; }
  inline void set_sample_info(const SampleInfo &sample_info) { sample_info_ = sample_info; }
  inline bool is_gi_above() const override { return gi_charged_; }
  inline void set_gi_above(bool gi_charged) { gi_charged_ = gi_charged; }
  inline bool is_sample_scan() const { return !sample_info_.is_no_sample(); }
  inline uint64_t get_location_table_id() const { return is_index_global_ ? index_table_id_ : ref_table_id_; }
  int is_table_get(bool &is_get) const;
  void set_session_id(const uint64_t  v) { session_id_ = v; }
  uint64_t get_session_id() const { return session_id_; }

  bool is_need_feedback() const;
  int set_table_scan_filters(const common::ObIArray<ObRawExpr *> &filters);
  inline common::ObIArray<ObRawExpr*> &get_range_conditions() { return range_conds_; }
  const common::ObIArray<ObRawExpr*> &get_range_conditions() const { return range_conds_; }
  inline void set_diverse_path_count(int64_t count) { diverse_path_count_ = count; }
  inline int64_t get_diverse_path_count() const { return diverse_path_count_; }
  inline TableItem::FlashBackQueryType get_flashback_query_type() const {return fq_type_; }
  inline void set_flashback_query_type(TableItem::FlashBackQueryType type) { fq_type_ = type; }
  inline bool get_fq_read_tx_uncommitted() const { return fq_read_tx_uncommitted_; }
  inline void set_fq_read_tx_uncommitted(bool v) { fq_read_tx_uncommitted_ = v; }
  inline const ObRawExpr* get_flashback_query_expr() const { return fq_expr_; }
  inline ObRawExpr* &get_flashback_query_expr() { return fq_expr_; }
  inline void set_flashback_query_expr(ObRawExpr *expr) { fq_expr_ = expr; }
  int add_mapping_column_for_vt(ObColumnRefRawExpr *col_expr,
                                ObRawExpr *&real_expr);
  int get_phy_location_type(ObTableLocationType &location_type);
  virtual int generate_access_exprs();
  int copy_filter_before_index_back();
  void set_use_batch(bool use_batch) { use_batch_ = use_batch; }
  bool use_batch() const { return use_batch_; }
  // use group_id_expr_ when batch rescan or keep order for global lookup.
  bool use_group_id() const { return use_batch_ || (is_index_global_ && index_back_ && das_keep_ordering_); }
  inline const ObRawExpr *get_group_id_expr() const { return group_id_expr_; }
  int extract_bnlj_param_idxs(common::ObIArray<int64_t> &bnlj_params);

  void set_access_path(AccessPath* path) { access_path_ = path; }
  inline const AccessPath* get_access_path() const { return access_path_; }
  void set_tablet_id_expr(ObOpPseudoColumnRawExpr *expr) { tablet_id_expr_ = expr; }
  void set_trans_info_expr(ObOpPseudoColumnRawExpr *expr) { trans_info_expr_ = expr; }
  void set_part_join_filter_created(bool flag) { part_join_filter_allocated_ = flag; }
  bool is_part_join_filter_created() { return part_join_filter_allocated_; }
  ObOpPseudoColumnRawExpr *get_tablet_id_expr() const { return tablet_id_expr_; }
  ObRawExpr *get_trans_info_expr() const { return trans_info_expr_; }
  void set_tablet_id_type(int64_t type) { tablet_id_type_ = type; }
  int64_t get_tablet_id_type() const { return tablet_id_type_; }
  const common::ObIArray<ObRawExpr*> &get_rowkey_exprs() const { return rowkey_exprs_; }
  const common::ObIArray<ObRawExpr*> &get_part_exprs() const { return part_exprs_; }
  inline const ObRawExpr *get_calc_part_id_expr() const { return calc_part_id_expr_; }
  int init_calc_part_id_expr();
  void set_table_type(share::schema::ObTableType table_type) { table_type_ = table_type; }
  share::schema::ObTableType get_table_type() const { return table_type_; }
  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
  int print_est_method(ObBaseTableEstMethod method, char *buf, int64_t &buf_len, int64_t &pos);
  int get_plan_object_info(PlanText &plan_text,
                           ObSqlPlanItem &plan_item);
  inline ObTablePartitionInfo *get_global_index_back_table_partition_info() { return global_index_back_table_partition_info_; }
  inline const ObTablePartitionInfo *get_global_index_back_table_partition_info() const { return global_index_back_table_partition_info_; }
  inline void set_global_index_back_table_partition_info(ObTablePartitionInfo *global_index_back_table_partition_info) { global_index_back_table_partition_info_ = global_index_back_table_partition_info; }
  inline bool has_index_scan_filter() { return has_index_scan_filter_; }
  inline void set_has_index_scan_filter(bool has_index_scan_filter) { has_index_scan_filter_ = has_index_scan_filter; }
  inline bool has_index_lookup_filter() { return has_index_lookup_filter_; }
  inline void set_has_index_lookup_filter(bool has_index_lookup_filter) { has_index_lookup_filter_ = has_index_lookup_filter; }
  int generate_ddl_output_column_ids();
  int replace_gen_col_op_exprs(ObRawExprReplacer &replacer);
  int extract_pushdown_filters(ObIArray<ObRawExpr*> &nonpushdown_filters,
                               ObIArray<ObRawExpr*> &scan_pushdown_filters,
                               ObIArray<ObRawExpr*> &lookup_pushdown_filters,
                               bool ignore_pd_filter = false) const;
  int has_nonpushdown_filter(bool &has_npd_filter);
  int replace_index_back_pushdown_filters(ObRawExprReplacer &replacer);
  int extract_virtual_gen_access_exprs(ObIArray<ObRawExpr*> &access_exprs,
                                      uint64_t scan_table_id);
  int adjust_print_access_info(ObIArray<ObRawExpr*> &access_exprs);
  static int replace_gen_column(ObLogPlan *plan, ObRawExpr *part_expr, ObRawExpr *&new_part_expr);
  int extract_file_column_exprs_recursively(ObRawExpr *expr);
  inline bool is_text_retrieval_scan() const { return is_index_scan() && NULL != text_retrieval_info_.match_expr_; }
  inline bool is_multivalue_index_scan() const { return is_multivalue_index_; }
  inline ObTextRetrievalInfo &get_text_retrieval_info() { return text_retrieval_info_; }
  inline const ObTextRetrievalInfo &get_text_retrieval_info() const { return text_retrieval_info_; }
  int prepare_text_retrieval_dep_exprs();
  inline bool need_text_retrieval_calc_relevance() const { return text_retrieval_info_.need_calc_relevance_; }
  inline bool need_doc_id_index_back() const { return is_text_retrieval_scan() || is_multivalue_index_scan() ; }
  inline void set_doc_id_index_table_id(const uint64_t doc_id_index_table_id) { doc_id_table_id_ = doc_id_index_table_id; }
  inline uint64_t get_doc_id_index_table_id() const { return doc_id_table_id_; }
  virtual int get_card_without_filter(double &card) override;
  inline ObRawExpr *get_identify_seq_expr() { return identify_seq_expr_; }
  inline int has_exec_param(bool &bool_ret) const
  {
    return est_cost_info_ == NULL ? common::OB_SUCCESS : est_cost_info_->has_exec_param(bool_ret);
  }
  void set_identify_seq_expr(ObRawExpr *expr) { identify_seq_expr_ = expr; }

  inline bool das_need_keep_ordering() const { return das_keep_ordering_; }

  int check_das_need_keep_ordering();

  const ObIArray<ObRawFilterMonotonicity>& get_filter_monotonicity() const
  { return filter_monotonicity_; }
  int get_filter_monotonicity(const ObRawExpr *filter,
                              const ObColumnRefRawExpr *col_expr,
                              PushdownFilterMonotonicity &mono,
                              ObIArray<ObRawExpr *> &assist_exprs) const;
private: // member functions
  //called when index_back_ set
  int pick_out_query_range_exprs();
  int filter_before_index_back_set();
  virtual int print_outline_data(PlanText &plan_text) override;
  virtual int print_used_hint(PlanText &plan_text) override;
  int print_range_annotation(char *buf, int64_t buf_len, int64_t &pos, ExplainType type);
  int print_filter_before_indexback_annotation(char *buf, int64_t buf_len, int64_t &pos);
  int print_limit_offset_annotation(char *buf, int64_t buf_len, int64_t &pos, ExplainType type);
  int print_ranges(char *buf, int64_t buf_len, int64_t &pos, const ObIArray<ObNewRange> &ranges);
  virtual int explain_index_selection_info(char *buf, int64_t &buf_len, int64_t &pos);
  int generate_necessary_rowkey_and_partkey_exprs();
  int add_mapping_columns_for_vt(ObIArray<ObRawExpr*> &access_exprs);
  int get_mbr_column_exprs(const uint64_t table_id, ObIArray<ObRawExpr *> &mbr_exprs);
  int allocate_lookup_trans_info_expr();
  int allocate_group_id_expr();
  int extract_doc_id_index_back_expr(ObIArray<ObRawExpr *> &exprs);
  int extract_text_retrieval_access_expr(ObIArray<ObRawExpr *> &exprs);
  int get_text_retrieval_calc_exprs(ObIArray<ObRawExpr *> &all_exprs);
  int print_text_retrieval_annotation(char *buf, int64_t buf_len, int64_t &pos, ExplainType type);
  int find_nearest_rcte_op(ObLogSet *&rcte_op);
  int generate_filter_monotonicity();
  int get_filter_assist_exprs(ObIArray<ObRawExpr *> &assist_exprs);
protected: // memeber variables
  // basic info
  uint64_t table_id_; //table id or alias table id
  uint64_t ref_table_id_; //base table id
  uint64_t index_table_id_;
  uint64_t session_id_; //for temporary table, record session id
  uint64_t advisor_table_id_; // used for duplicate table replica selection in the plan cache
  bool is_index_global_;
  bool is_spatial_index_;
  bool is_multivalue_index_;
  // TODO yuming: tells whether the table scan uses shared data access or not
  // mainly designed for code generator
  bool use_das_;
  //currently index back only mean tsc is local index lookup, not contain global index
  bool index_back_;
  bool is_multi_part_table_scan_;
  common::ObString table_name_;
  common::ObString index_name_;
  ObOrderDirection scan_direction_;
  bool      for_update_;       // FOR UPDATE clause
  int64_t for_update_wait_us_; // 0 means nowait, -1 means infinite
  // query range after preliminary extract, which will be stored in physical plan
  // for future use
  const ObQueryRange *pre_query_range_;
  const common::ObIArray<int64_t> *part_ids_;
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> range_conds_;

  // index primary key columns.
  // indicates use which columns to extract query range
  common::ObSEArray<ColumnItem, 4, common::ModulePageAllocator, true> range_columns_;
  // index all columns, including storing columns
  common::ObSEArray<uint64_t, 5, common::ModulePageAllocator, true> idx_columns_;
  // base columns to scan
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> access_exprs_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> rowkey_exprs_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> part_exprs_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> spatial_exprs_;
  // columns required for accessing a domain index (fulltext and JSON multi-value index)
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> domain_exprs_;
  //for external table
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> ext_file_column_exprs_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> ext_column_convert_exprs_;
  // for oracle-mapping, map access expr to a real column expr
  common::ObArray<std::pair<ObRawExpr *, ObRawExpr *>, common::ModulePageAllocator, true> real_expr_map_;
  // aggr func pushdwon to table scan
  common::ObSEArray<ObAggFunRawExpr *, 4, common::ModulePageAllocator, true> pushdown_aggr_exprs_;
  // group by columns pushdown to table scan
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> pushdown_groupby_columns_;
  // whether a filter can be evaluated before index back
  common::ObSEArray<bool, 4, common::ModulePageAllocator, true> filter_before_index_back_;
// // removal these in cg layer, up to opt layer.
  common::ObSEArray<uint64_t, 4, common::ModulePageAllocator, true> ddl_output_column_ids_;
// removal these in cg layer, up to opt layer end.
  // table partition locations
  ObTablePartitionInfo *table_partition_info_; //this member is not in copy_without_child,
                                               //because its used in EXCHANGE stage, and
                                               //copy_without_child used before this
  ObRangesArray ranges_;//For explain. Code generator and executor cannot use this.
  ObRangesArray ss_ranges_;//For explain. Code generator and executor cannot use this.
  bool is_skip_scan_;

  // limit params from upper limit op
  ObRawExpr *limit_count_expr_;
  ObRawExpr *limit_offset_expr_;
  // 记录该表是否采样、采样方式、比例等信息
  SampleInfo sample_info_;
  ObCostTableScanInfo *est_cost_info_;
  BaseTableOptInfo *table_opt_info_;
  common::ObSEArray<common::ObEstRowCountRecord, 4, common::ModulePageAllocator, true> est_records_;

  ObRawExpr *part_expr_;
  ObRawExpr *subpart_expr_;

  // whether alloc a granule iterator.
  // and this var will transmit to phy tsc,
  // phy tsc will skip the do_table_scan at inner_open
  bool gi_charged_;
  // if a table scan is in a partition wise join subplan,
  // we do not alloc a gi above this op.
  bool gi_alloc_post_state_forbidden_;

  int64_t diverse_path_count_; // count of access path with diverse query ranges

  ObRawExpr* fq_expr_; //flashback query expr
  TableItem::FlashBackQueryType fq_type_; //flashback query type
  bool fq_read_tx_uncommitted_; // whether flashback query read uncommitted changes in transaction
   // for join partition filter
  ObPxBFStaticInfo bf_info_;
  bool part_join_filter_allocated_;
  // end for partition join filter
  ObRawExpr *group_id_expr_;
  bool use_batch_;
  AccessPath *access_path_;
  ObOpPseudoColumnRawExpr *tablet_id_expr_;
  // decide tablet_id_expr should reture which id
  // 0 for tablet id, 1 for logical part id, 2 for logical subpart id
  int64_t tablet_id_type_;
  ObRawExpr *calc_part_id_expr_;
  ObRawExpr *trans_info_expr_;

  //for batch search recursive cte
  ObRawExpr *identify_seq_expr_;

  // begin for global index lookup
  ObTablePartitionInfo *global_index_back_table_partition_info_;
  bool has_index_scan_filter_;
  bool has_index_lookup_filter_;
  // end for global index lookup

  share::schema::ObTableType table_type_;
  bool use_column_store_;
  uint64_t doc_id_table_id_; // used for rowkey lookup of fulltext and JSON multi-value index
  ObTextRetrievalInfo text_retrieval_info_;

  ObPxRFStaticInfo px_rf_info_;
  bool das_keep_ordering_;
  typedef common::ObSEArray<ObRawFilterMonotonicity, 4, common::ModulePageAllocator, true> FilterMonotonicity;
  FilterMonotonicity filter_monotonicity_;
  // disallow copy and assign
  DISALLOW_COPY_AND_ASSIGN(ObLogTableScan);
};
} // end of namespace sql
} // end of namespace oceanbase

#endif /* _OB_LOG_TABLE_SCAN_H */
