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

namespace oceanbase {
namespace sql {
class ObIndexHint;
class Path;

class ObLogTableScan : public ObLogicalOperator {
  // ObLogTableLookup has a log tsc to read remote data table.
  friend class ObLogTableLookup;

public:
  ObLogTableScan(ObLogPlan& plan)
      : ObLogicalOperator(plan),
        table_id_(common::OB_INVALID_ID),
        ref_table_id_(common::OB_INVALID_ID),
        index_table_id_(common::OB_INVALID_ID),
        session_id_(0),
        is_index_global_(false),
        is_global_index_back_(false),
        is_fake_cte_table_(false),
        index_back_(false),
        is_multi_part_table_scan_(false),
        table_name_(),
        index_name_(),
        scan_direction_(default_asc_direction()),
        for_update_(false),
        for_update_wait_us_(-1), /* default infinite */
        hint_(),
        exist_hint_(false),
        pre_query_range_(NULL),
        part_hint_(NULL),
        filter_before_index_back_(),
        table_partition_info_(NULL),
        ranges_(),
        limit_count_expr_(NULL),
        limit_offset_expr_(NULL),
        sample_info_(),
        is_parallel_(false),
        index_back_cost_(0.0),
        est_cost_info_(NULL),
        table_row_count_(0),
        output_row_count_(0),
        phy_query_range_row_count_(0),
        query_range_row_count_(0),
        index_back_row_count_(0),
        estimate_method_(INVALID_METHOD),
        table_opt_info_(NULL),
        est_records_(),
        expected_part_id_(NULL),
        part_filter_(NULL),
        part_expr_(NULL),
        subpart_expr_(NULL),
        gi_charged_(false),
        gi_alloc_post_state_forbidden_(false),
        table_phy_location_type_(ObTableLocationType::OB_TBL_LOCATION_UNINITIALIZED),
        rowkey_cnt_(0),
        diverse_path_count_(0),
        extra_access_exprs_for_lob_col_()
  {}

  virtual ~ObLogTableScan()
  {}

  const char* get_name() const;

  // not used at the moment
  TO_STRING_KV(K_(table_id), K_(index_table_id), K_(is_fake_cte_table), K_(table_name), K_(index_name));
  /**
   *  Get table id
   */
  inline uint64_t get_table_id() const
  {
    return table_id_;
  }

  /**
   *  Get ref table id
   */
  inline uint64_t get_ref_table_id() const
  {
    return ref_table_id_;
  }

  /*
   * Get physical table location type
   */
  inline ObTableLocationType get_phy_location_type() const
  {
    return table_phy_location_type_;
  }

  void set_phy_location_type(const ObTableLocationType type)
  {
    table_phy_location_type_ = type;
  }

  /*
   * get is global index
   */
  inline uint64_t get_is_index_global() const
  {
    return is_index_global_;
  }
  /**
   *  Get index table id
   */
  inline uint64_t get_index_table_id() const
  {
    return index_table_id_;
  }

  inline bool get_is_global_index_back() const
  {
    return is_global_index_back_;
  }

  /**
   *  Set is fake cte table
   */
  inline bool get_is_fake_cte_table() const
  {
    return is_fake_cte_table_;
  }

  /**
   *  Get scan direction
   */
  inline ObOrderDirection get_scan_directioin() const
  {
    return scan_direction_;
  }

  /**
   *  Get pre query range
   */
  inline const ObQueryRange* get_pre_query_range() const
  {
    return pre_query_range_;
  }

  /**
   *  Get range columns
   */
  inline const common::ObIArray<ColumnItem>& get_range_columns() const
  {
    return range_columns_;
  }

  const common::ObIArray<OrderItem>& get_order_with_parts() const
  {
    return parts_sortkey_;
  }

  /**
   *  Set table id
   */
  inline void set_table_id(uint64_t table_id)
  {
    table_id_ = table_id;
  }

  /**
   *  Set ref table id
   */
  inline void set_ref_table_id(uint64_t ref_table_id)
  {
    ref_table_id_ = ref_table_id;
    set_dblink_id(extract_dblink_id(ref_table_id));
  }

  /**
   *  Set index table id
   */
  inline void set_index_table_id(uint64_t index_table_id)
  {
    index_table_id_ = index_table_id;
  }

  /*
   * set is global index id
   */
  inline void set_is_index_global(bool is_index_global)
  {
    is_index_global_ = is_index_global;
  }

  inline void set_is_global_index_back(bool is_global_index_back)
  {
    is_global_index_back_ = is_global_index_back;
  }

  /**
   *  Set is fake cte table
   */
  inline void set_is_fake_cte_table(bool is_fake_cte_table)
  {
    is_fake_cte_table_ = is_fake_cte_table;
  }

  /**
   *  Set scan direction
   */
  inline void set_scan_direction(ObOrderDirection direction)
  {
    scan_direction_ = direction;
    common::ObIArray<OrderItem>& op_ordering = get_op_ordering();
    for (int64_t i = 0; i < op_ordering.count(); ++i) {
      op_ordering.at(i).order_type_ = scan_direction_;
    }
  }

  /**
   *  Set pre query range
   */
  inline void set_pre_query_range(const ObQueryRange* query_range)
  {
    pre_query_range_ = query_range;
  }

  /**
   *  Set range columns
   */
  int set_range_columns(const common::ObIArray<ColumnItem>& range_columns);
  void set_rowkey_cnt(int64_t rowkey_cnt)
  {
    rowkey_cnt_ = rowkey_cnt;
  }
  inline int64_t get_rowkey_cnt() const
  {
    return rowkey_cnt_;
  }
  int add_idx_column_id(const uint64_t column_id)
  {
    return idx_columns_.push_back(column_id);
  }

  const common::ObIArray<uint64_t>& get_idx_columns() const
  {
    return idx_columns_;
  }

  void set_est_cost_info(ObCostTableScanInfo* param)
  {
    est_cost_info_ = param;
  }

  const ObCostTableScanInfo* get_est_cost_info() const
  {
    return est_cost_info_;
  }

  ObCostTableScanInfo* get_est_cost_info()
  {
    return est_cost_info_;
  }

  int set_update_info();

  void set_part_hint(const ObPartHint* part_hint)
  {
    part_hint_ = part_hint;
  }
  const ObPartHint* get_part_hint()
  {
    return part_hint_;
  }

  void set_part_expr(ObRawExpr* part_expr)
  {
    part_expr_ = part_expr;
  }
  ObRawExpr* get_part_expr() const
  {
    return part_expr_;
  }
  void set_subpart_expr(ObRawExpr* subpart_expr)
  {
    subpart_expr_ = subpart_expr;
  }
  ObRawExpr* get_subpart_expr() const
  {
    return subpart_expr_;
  }

  // should check index back after project pruning.Get final index back.
  virtual int index_back_check();

  virtual int generate_access_exprs(ObIArray<ObRawExpr*>& access_exprs);

  virtual int generate_rowkey_and_partkey_exprs(ObIArray<ObRawExpr*>& rowkey_exprs, bool has_lob_col = false);

  int extract_access_exprs(const share::schema::ObTableSchema* table_schema, const ObRowkeyInfo& rowkey_info,
      ObIArray<ObRawExpr*>& rowkey_exprs);

  /**
   *  Get access expressions
   */
  inline const common::ObIArray<ObRawExpr*>& get_access_exprs() const
  {
    return access_exprs_;
  }

  /**
   *  Get access expressions
   */
  inline common::ObIArray<ObRawExpr*>& get_access_exprs()
  {
    return access_exprs_;
  }

  /**
   *  Copy operator and it's properties without copying its child operators
   */
  virtual int copy_without_child(ObLogicalOperator*& out);

  /**
   * Generate the filtering expressions
   */
  int gen_filters();

  /**
   *  Allocate exchange nodes post-order function
   */
  virtual int allocate_exchange_post(AllocExchContext* ctx) override;

  /**
   *  Allocate granule iterator
   * */
  virtual int allocate_granule_pre(AllocGIContext& ctx) override;
  /**
   *  Allocate granule iterator
   * */
  virtual int allocate_granule_post(AllocGIContext& ctx) override;

  virtual int compute_property(Path* path);
  virtual int re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est) override;

  int get_index_cost(int64_t column_count, bool index_back, double& cost);

  virtual int allocate_expr_pre(ObAllocExprContext& ctx) override;
  /**
   * This function add all output columns from column items.
   */
  virtual int allocate_expr_post(ObAllocExprContext& ctx);

  /**
   *  Generate hash value for the operator using given seed
   */
  virtual uint64_t hash(uint64_t seed) const;

  /**
   *  Get table name
   */
  inline common::ObString& get_table_name()
  {
    return table_name_;
  }
  inline const common::ObString& get_table_name() const
  {
    return table_name_;
  }
  inline void set_table_name(const common::ObString& table_name)
  {
    table_name_ = table_name;
  }

  /**
   *  Get index name
   */
  inline const common::ObString& get_index_name() const
  {
    return index_name_;
  }

  inline common::ObString& get_index_name()
  {
    return index_name_;
  }

  inline void set_index_name(common::ObString& index_name)
  {
    index_name_ = index_name;
  }

  /**
   *
   */
  inline ObTablePartitionInfo* get_table_partition_info_for_update()
  {
    return table_partition_info_;
  }
  inline const ObTablePartitionInfo* get_table_partition_info() const
  {
    return table_partition_info_;
  }
  inline void set_table_partition_info(ObTablePartitionInfo* table_partition_info)
  {
    table_partition_info_ = table_partition_info;
  }

  virtual int init_table_location_info();
  int is_prefix_of_partition_key(ObIArray<OrderItem>& ordering, bool& is_prefix);

  virtual int generate_link_sql_pre(GenLinkStmtContext& link_ctx) override;

  bool is_index_scan() const
  {
    return ref_table_id_ != index_table_id_;
  }
  bool is_table_whole_range_scan() const
  {
    return !is_index_scan() && (NULL == pre_query_range_ || (1 == ranges_.count() && ranges_.at(0).is_whole_range()));
  }
  virtual bool is_table_scan() const override
  {
    return true;
  }
  bool is_whole_range_scan() const
  {
    return NULL == pre_query_range_ || (1 == ranges_.count() && ranges_.at(0).is_whole_range());
  }
  void set_for_update(bool for_update, int64_t wait_us)
  {
    for_update_ = for_update;
    for_update_wait_us_ = wait_us;
  }
  bool is_for_update() const
  {
    return for_update_;
  }
  int64_t get_for_update_wait_us() const
  {
    return for_update_wait_us_;
  }
  ObOrderDirection get_scan_direction() const
  {
    return scan_direction_;
  }
  void set_index_back(bool index_back)
  {
    index_back_ = index_back;
  }
  bool get_index_back()
  {
    return index_back_;
  }
  void set_is_multi_part_table_scan(bool multi_part_tsc)
  {
    is_multi_part_table_scan_ = multi_part_tsc;
  }
  bool get_is_multi_part_table_scan()
  {
    return is_multi_part_table_scan_;
  }
  int set_query_ranges(ObRangesArray ranges);

  void set_hint(common::ObTableScanHint& hint)
  {
    hint_ = hint;
  }
  const common::ObTableScanHint& get_const_hint() const
  {
    return hint_;
  }
  common::ObTableScanHint& get_hint()
  {
    return hint_;
  }
  void set_exist_hint(bool exist_hint)
  {
    exist_hint_ = exist_hint;
  }
  bool exist_hint()
  {
    return exist_hint_;
  }
  virtual int inner_replace_generated_agg_expr(
      const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& to_replace_exprs);
  inline common::ObIArray<bool>& get_filter_before_index_flags()
  {
    return filter_before_index_back_;
  }
  inline const common::ObIArray<bool>& get_filter_before_index_flags() const
  {
    return filter_before_index_back_;
  }
  inline ObRawExpr* get_limit_expr()
  {
    return limit_count_expr_;
  }
  inline ObRawExpr* get_offset_expr()
  {
    return limit_offset_expr_;
  }
  inline void set_limit_offset(ObRawExpr* limit, ObRawExpr* offset)
  {
    limit_count_expr_ = limit;
    limit_offset_expr_ = offset;
  }
  inline void set_table_row_count(int64_t table_row_count)
  {
    table_row_count_ = table_row_count;
  }
  inline int64_t get_table_row_count() const
  {
    return table_row_count_;
  }
  inline void set_output_row_count(double output_row_count)
  {
    output_row_count_ = output_row_count;
  }
  inline double get_output_row_count() const
  {
    return output_row_count_;
  }
  inline void set_phy_query_range_row_count(double phy_query_range_row_count)
  {
    phy_query_range_row_count_ = phy_query_range_row_count;
  }
  inline double get_phy_query_range_row_count() const
  {
    return phy_query_range_row_count_;
  }
  inline void set_query_range_row_count(double query_range_row_count)
  {
    query_range_row_count_ = query_range_row_count;
  }
  inline double get_query_range_row_count() const
  {
    return query_range_row_count_;
  }
  inline void set_index_back_row_count(double index_back_row_count)
  {
    index_back_row_count_ = index_back_row_count;
  }
  inline double get_index_back_row_count() const
  {
    return index_back_row_count_;
  }
  inline void set_estimate_method(RowCountEstMethod method)
  {
    estimate_method_ = method;
  }
  inline RowCountEstMethod get_estimate_method() const
  {
    return estimate_method_;
  }
  int set_limit_size();
  int is_top_table_scan(bool& is_top_table_scan)
  {
    int ret = common::OB_SUCCESS;
    is_top_table_scan = false;
    if (NULL == get_parent()) {
      is_top_table_scan = true;
    } else if (log_op_def::LOG_EXCHANGE == get_parent()->get_type() && OB_ISNULL(get_parent()->get_parent())) {
      ret = common::OB_ERR_UNEXPECTED;
    } else if (log_op_def::LOG_EXCHANGE == get_parent()->get_type() &&
               NULL == get_parent()->get_parent()->get_parent()) {
      is_top_table_scan = true;
    } else { /* Do nothing */
    }
    return ret;
  }
  int get_path_ordering(common::ObIArray<ObRawExpr*>& order_exprs);

  inline void set_table_opt_info(BaseTableOptInfo* table_opt_info)
  {
    table_opt_info_ = table_opt_info;
  }

  int set_est_row_count_record(common::ObIArray<common::ObEstRowCountRecord>& est_records)
  {
    return est_records_.assign(est_records);
  }

  int set_query_range_exprs(const common::ObIArray<ObRawExpr*>& range_exprs)
  {
    return range_conds_.assign(range_exprs);
  }

  int set_extra_access_exprs_for_lob_col(const common::ObIArray<ObRawExpr*>& exprs)
  {
    return extra_access_exprs_for_lob_col_.assign(exprs);
  }

  inline BaseTableOptInfo* get_table_opt_info()
  {
    return table_opt_info_;
  }

  inline const common::ObIArray<common::ObEstRowCountRecord>& get_est_row_count_record() const
  {
    return est_records_;
  }

  inline ObRawExpr* get_expected_part_id()
  {
    return expected_part_id_;
  }

  inline SampleInfo& get_sample_info()
  {
    return sample_info_;
  }
  inline const SampleInfo& get_sample_info() const
  {
    return sample_info_;
  }
  inline void set_sample_info(const SampleInfo& sample_info)
  {
    sample_info_ = sample_info;
  }
  inline bool is_gi_above()
  {
    return gi_charged_;
  }
  inline void set_gi_above(bool gi_charged)
  {
    gi_charged_ = gi_charged;
  }
  inline bool is_sample_scan() const
  {
    return !sample_info_.is_no_sample();
  }
  inline void set_index_back_cost(const double index_back_cost)
  {
    index_back_cost_ = index_back_cost;
  }
  inline double get_index_back_cost() const
  {
    return index_back_cost_;
  }
  inline double get_index_back_cost()
  {
    return index_back_cost_;
  }
  inline uint64_t get_location_table_id() const
  {
    return is_index_global_ ? index_table_id_ : ref_table_id_;
  }
  int is_table_get(bool& is_get) const;
  void set_session_id(const uint64_t v)
  {
    session_id_ = v;
  }
  uint64_t get_session_id() const
  {
    return session_id_;
  }

  virtual int transmit_op_ordering();
  virtual int transmit_local_ordering();
  bool is_need_feedback() const;
  int set_filters(const common::ObIArray<ObRawExpr*>& filters);
  int refine_query_range();
  inline common::ObIArray<ObRawExpr*>& get_range_conditions()
  {
    return range_conds_;
  }

  inline void set_diverse_path_count(int64_t count)
  {
    diverse_path_count_ = count;
  }
  inline int64_t get_diverse_path_count() const
  {
    return diverse_path_count_;
  }
  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const override;

  int get_duplicate_table_replica(ObIArray<ObAddr>& valid_addrs) override;
  int set_duplicate_table_replica(const ObAddr& addr) override;

  int add_mapping_column_for_vt(ObColumnRefRawExpr* col_expr);

private:  // member functions
  // called when index_back_ set
  int pick_out_query_range_exprs();
  int pick_out_startup_filters();
  int filter_before_index_back_set();
  bool is_covered(ObRelIds& rel_ids);
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);
  //  virtual int print_outline(char *buf, int64_t &buf_len, int64_t &pos, bool is_oneline);
  virtual int print_outline(planText& plan_text);
  int print_outline_data(planText& plan_text);
  int print_all_no_index(const ObIndexHint& index_hint, planText& plan_text);
  int print_single_no_index(const ObString& index_name, planText& plan_text);
  int get_index_hint(bool& is_used_hint, const ObIndexHint*& index_hint);
  int print_used_index(planText& plan_text);
  virtual int print_operator_for_outline(planText& plan_text);
  virtual int is_used_join_type_hint(JoinAlgo join_algo, bool& is_used);
  virtual int is_used_in_leading_hint(bool& is_used);
  int print_no_use_late_materialization(planText& plan_text);
  int print_range_annotation(char* buf, int64_t buf_len, int64_t& pos, ExplainType type);
  int print_filter_before_indexback_annotation(char* buf, int64_t buf_len, int64_t& pos);
  int print_limit_offset_annotation(char* buf, int64_t buf_len, int64_t& pos, ExplainType type);
  int print_ranges(char* buf, int64_t buf_len, int64_t& pos, const ObIArray<ObNewRange>& ranges);
  int print_split_range_annotation(char* buf, int64_t buf_len, int64_t& pos, ExplainType type);
  virtual int explain_index_selection_info(char* buf, int64_t& buf_len, int64_t& pos);
  int generate_part_filter(ObRawExpr*& part_filter_expr);
  int fill_link_stmt(
      const common::ObIArray<ObRawExpr*>& select_strs, const TableItem& table_item, ObLinkStmt& link_stmt);

  int add_mapping_columns_for_vt(ObIArray<ObRawExpr*>& access_exprs);
  virtual int check_output_dep_specific(ObRawExprCheckDep& checker) override;
  int try_add_extra_access_exprs_for_lob_col();

protected:  // memeber variables
  // basic info
  uint64_t table_id_;      // table id or alias table id
  uint64_t ref_table_id_;  // base table id
  uint64_t index_table_id_;
  uint64_t session_id_;  // for temporary table, record session id
  bool is_index_global_;
  bool is_global_index_back_;
  bool is_fake_cte_table_;
  bool index_back_;
  bool is_multi_part_table_scan_;
  common::ObString table_name_;
  common::ObString index_name_;
  ObOrderDirection scan_direction_;
  bool for_update_;             // FOR UPDATE clause
  int64_t for_update_wait_us_;  // 0 means nowait, -1 means infinite

  common::ObTableScanHint hint_;
  bool exist_hint_;
  // query range after preliminary extract, which will be stored in physical plan
  // for future use
  const ObQueryRange* pre_query_range_;
  const ObPartHint* part_hint_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> range_conds_;

  // index primary key columns.
  // indicates use which columns to extract query range
  common::ObSEArray<ColumnItem, 4, common::ModulePageAllocator, true> range_columns_;
  // index all columns, including storing columns
  common::ObSEArray<uint64_t, 5, common::ModulePageAllocator, true> idx_columns_;
  common::ObSEArray<OrderItem, 5, common::ModulePageAllocator, true> parts_sortkey_;  // ordering with partitions
                                                                                      // considering
  // base columns to scan
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> access_exprs_;
  // whether a filter can be evaluated before index back
  common::ObSEArray<bool, 4, common::ModulePageAllocator, true> filter_before_index_back_;

  // table partiton locations
  ObTablePartitionInfo* table_partition_info_;  // this member is not in copy_without_child,
                                                // because its used in EXCHANGE stage, and
                                                // copy_without_child used before this
  ObRangesArray ranges_;                        // For explain. Code generator and executor cannot use this.

  // limit params from upper limit op
  ObRawExpr* limit_count_expr_;
  ObRawExpr* limit_offset_expr_;
  SampleInfo sample_info_;
  bool is_parallel_;
  /*for re-estimate purpose*/
  double index_back_cost_;
  ObCostTableScanInfo* est_cost_info_;
  int64_t table_row_count_;
  double output_row_count_;
  double phy_query_range_row_count_;
  double query_range_row_count_;
  double index_back_row_count_;
  RowCountEstMethod estimate_method_;
  BaseTableOptInfo* table_opt_info_;
  common::ObSEArray<common::ObEstRowCountRecord, 4, common::ModulePageAllocator, true> est_records_;

  ObRawExpr* expected_part_id_;
  ObRawExpr* part_filter_;

  ObRawExpr* part_expr_;
  ObRawExpr* subpart_expr_;

  // whether alloc a granule iterator.
  // and this var will transmit to phy tsc,
  // phy tsc will skip the do_table_scan at inner_open
  bool gi_charged_;
  // if a table scan is in a partition wise join subplan,
  // we do not alloc a gi above this op.
  bool gi_alloc_post_state_forbidden_;
  // for plan cache
  ObTableLocationType table_phy_location_type_;
  int64_t rowkey_cnt_;

  int64_t diverse_path_count_;  // count of access path with diverse query ranges

  // if we got lob column, need to project all column that rowid column need
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> extra_access_exprs_for_lob_col_;
  // disallow copy and assign
  DISALLOW_COPY_AND_ASSIGN(ObLogTableScan);
};
}  // end of namespace sql
}  // end of namespace oceanbase

#endif /* _OB_LOG_TABLE_SCAN_H */
