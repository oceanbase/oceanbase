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

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_COST_
#define OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_COST_
#include "lib/container/ob_array.h"
#include "common/object/ob_object.h"
#include "share/stat/ob_stat_manager.h"
#include "share/ob_simple_batch.h"
#include "share/ob_rpc_struct.h"
#include "sql/rewrite/ob_query_range_provider.h"
#include "sql/optimizer/ob_opt_est_sel.h"
#include "sql/optimizer/ob_opt_default_stat.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "storage/ob_range_iterator.h"

namespace oceanbase {
namespace storage {
class ObIPartitionGroupGuard;
}
namespace sql {
class ObDMLStmt;
class JoinPath;
class OrderItem;
struct ObExprSelPair;

enum RowCountEstMethod {
  INVALID_METHOD = 0,
  REMOTE_STORAGE,  // use remote storage layer to estimate row count
  LOCAL_STORAGE,   // use local storage layer to estimate row count
  BASIC_STAT,      // use min/max/ndv to estimate row count
  HISTOGRAM        // place holder
};

// all the table meta info need to compute cost
struct ObTableMetaInfo {
  ObTableMetaInfo(uint64_t ref_table_id)
      : ref_table_id_(ref_table_id),
        schema_version_(share::OB_INVALID_SCHEMA_VERSION),
        part_count_(0),
        micro_block_size_(0),
        part_size_(0),
        average_row_size_(0),
        table_column_count_(0),
        table_rowkey_count_(0),
        table_row_count_(0),
        row_count_(0),
        is_only_memtable_data_(false),
        cost_est_type_(ObEstimateType::OB_CURRENT_STAT_EST)
  {}
  virtual ~ObTableMetaInfo()
  {}

  void assign(const ObTableMetaInfo& table_meta_info);
  TO_STRING_KV(K_(ref_table_id), K_(part_count), K_(micro_block_size), K_(part_size), K_(average_row_size),
      K_(table_column_count), K_(table_rowkey_count), K_(table_row_count), K_(row_count), K_(is_only_memtable_data),
      K_(cost_est_type), K_(table_est_part));
  uint64_t ref_table_id_;       // ref table id
  int64_t schema_version_;      // schema version
  int64_t part_count_;          // partition count
  int64_t micro_block_size_;    // main table micro block size
  double part_size_;            // main table best partition data size
  double average_row_size_;     // main table best partition average row size
  int64_t table_column_count_;  // table column count
  int64_t table_rowkey_count_;
  int64_t table_row_count_;     // table row count in stat.
  double row_count_;            // row count after filters, estimated by stat manager
  bool is_only_memtable_data_;  // whether has only memtable data
  EstimatedPartition table_est_part_;
  ObEstimateType cost_est_type_;  // cost estimation type
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableMetaInfo);
};

// all the index meta info need to compute cost
struct ObIndexMetaInfo {
  ObIndexMetaInfo(uint64_t ref_table_id, uint64_t index_id)
      : ref_table_id_(ref_table_id),
        index_id_(index_id),
        index_micro_block_size_(0),
        index_part_size_(0),
        index_column_count_(0),
        is_index_back_(false),
        is_unique_index_(false),
        is_global_index_(false)
  {}
  virtual ~ObIndexMetaInfo()
  {}
  void assign(const ObIndexMetaInfo& index_meta_info);
  TO_STRING_KV(K_(ref_table_id), K_(index_id), K_(index_micro_block_size), K_(index_part_size), K_(index_column_count),
      K_(is_index_back), K_(is_unique_index));
  uint64_t ref_table_id_;           // ref table id
  uint64_t index_id_;               // index id
  int64_t index_micro_block_size_;  // index micro block size, same as main table when path is primary
  double index_part_size_;          // index table partitoin(0) data size, same as main table when path is primary
  int64_t index_column_count_;      // index column count
  bool is_index_back_;              // is index back
  bool is_unique_index_;            // is unique index
  bool is_global_index_;            // whether is global index
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndexMetaInfo);
};

struct ObBasicCostInfo {
  ObBasicCostInfo() : rows_(0), cost_(0), width_(0)
  {}
  ObBasicCostInfo(double rows, double cost, double width) : rows_(rows), cost_(cost), width_(width)
  {}
  TO_STRING_KV(K_(rows), K_(cost), K_(width));
  double rows_;
  double cost_;
  double width_;
};

struct ObTwoChildrenNodeCostInfo {
  ObTwoChildrenNodeCostInfo(
      double left_rows, double left_cost, double left_width, double right_rows, double right_cost, double right_width)
      : left_rows_(left_rows),
        left_cost_(left_cost),
        left_width_(left_width),
        right_rows_(right_rows),
        right_cost_(right_cost),
        right_width_(right_width)
  {}
  const double left_rows_;
  const double left_cost_;
  const double left_width_;
  const double right_rows_;
  const double right_cost_;
  const double right_width_;
};
/*
 * store all the info needed to cost table scan
 */
struct ObCostTableScanInfo {
  ObCostTableScanInfo(uint64_t table_id, uint64_t ref_table_id, uint64_t index_id)
      : table_id_(table_id),
        ref_table_id_(ref_table_id),
        index_id_(index_id),
        table_meta_info_(ref_table_id),
        index_meta_info_(ref_table_id, index_id),
        is_virtual_table_(is_virtual_table(ref_table_id)),
        is_unique_(false),
        ranges_(),
        range_columns_(),
        prefix_filters_(),
        pushdown_prefix_filters_(),
        postfix_filters_(),
        table_filters_(),
        table_scan_param_(),
        est_sel_info_(NULL),
        row_est_method_(RowCountEstMethod::INVALID_METHOD),
        prefix_filter_sel_(1.0),
        pushdown_prefix_filter_sel_(1.0),
        postfix_filter_sel_(1.0),
        table_filter_sel_(1.0),
        batch_type_(common::ObSimpleBatch::ObBatchType::T_NONE)
  {}
  virtual ~ObCostTableScanInfo()
  {}

  int assign(const ObCostTableScanInfo& other_est_cost_info);

  const ObEstSelInfo* get_est_sel_info() const
  {
    return est_sel_info_;
  }

  TO_STRING_KV(K_(table_id), K_(ref_table_id), K_(index_id), K_(table_meta_info), K_(index_meta_info),
      K_(is_virtual_table), K_(is_unique), K_(prefix_filter_sel), K_(pushdown_prefix_filter_sel),
      K_(postfix_filter_sel), K_(table_filter_sel));
  // the following information need to be set before estimating cost
  uint64_t table_id_;                                                                  // table id
  uint64_t ref_table_id_;                                                              // ref table id
  uint64_t index_id_;                                                                  // index_id
  ObTableMetaInfo table_meta_info_;                                                    // table related meta info
  ObIndexMetaInfo index_meta_info_;                                                    // index related meta info
  bool is_virtual_table_;                                                              // is virtual table
  bool is_unique_;                                                                     // whether query range is unique
  ObRangesArray ranges_;                                                               // all the ranges
  common::ObSEArray<ColumnItem, 4, common::ModulePageAllocator, true> range_columns_;  // all the range columns

  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> prefix_filters_;  // filters match index prefix
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true>
      pushdown_prefix_filters_;  // filters match index prefix along pushed down filter
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true>
      postfix_filters_;  // filters evaluated before index back, but not index prefix
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> table_filters_;  // filters evaluated after index
                                                                                       // back

  storage::ObTableScanParam table_scan_param_;
  ObEstSelInfo* est_sel_info_;
  // the following information are useful when estimating cost
  RowCountEstMethod row_est_method_;  // row_est_method
  double prefix_filter_sel_;
  double pushdown_prefix_filter_sel_;
  double postfix_filter_sel_;
  double table_filter_sel_;
  common::ObSimpleBatch::ObBatchType batch_type_;
  SampleInfo sample_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCostTableScanInfo);
};

struct ObCostBaseJoinInfo : public ObTwoChildrenNodeCostInfo {
  ObCostBaseJoinInfo(double left_rows, double left_cost, double left_width, double right_rows, double right_cost,
      double right_width, ObRelIds left_ids, ObRelIds right_ids, ObJoinType join_type,
      const common::ObIArray<ObRawExpr*>& equal_join_condition,
      const common::ObIArray<ObRawExpr*>& other_join_condition, ObEstSelInfo* est_sel_info)
      : ObTwoChildrenNodeCostInfo(left_rows, left_cost, left_width, right_rows, right_cost, right_width),
        left_ids_(left_ids),
        right_ids_(right_ids),
        join_type_(join_type),
        equal_join_condition_(equal_join_condition),
        other_join_condition_(other_join_condition),
        est_sel_info_(est_sel_info)
  {}
  virtual ~ObCostBaseJoinInfo(){};

  const ObEstSelInfo* get_est_sel_info() const
  {
    return est_sel_info_;
  }

  TO_STRING_KV(K_(left_rows), K_(left_cost), K_(right_rows), K_(right_cost), K_(left_width), K_(right_width),
      K_(left_ids), K_(right_ids), K_(join_type), K_(equal_join_condition), K_(other_join_condition));
  ObRelIds left_ids_;
  ObRelIds right_ids_;
  ObJoinType join_type_;
  const common::ObIArray<ObRawExpr*>& equal_join_condition_;
  const common::ObIArray<ObRawExpr*>& other_join_condition_;
  ObEstSelInfo* est_sel_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCostBaseJoinInfo);
};

struct ObCostNLJoinInfo : public ObCostBaseJoinInfo {
  ObCostNLJoinInfo(double left_rows, double left_cost, double left_width, double right_rows, double right_cost,
      double right_width, ObRelIds left_ids, ObRelIds right_ids, ObJoinType join_type, double anti_or_semi_match_sel,
      bool with_nl_param, bool need_mat, const common::ObIArray<ObRawExpr*>& equal_join_condition,
      const common::ObIArray<ObRawExpr*>& other_join_condition, ObEstSelInfo* est_sel_info)
      : ObCostBaseJoinInfo(left_rows, left_cost, left_width, right_rows, right_cost, right_width, left_ids, right_ids,
            join_type, equal_join_condition, other_join_condition, est_sel_info),
        anti_or_semi_match_sel_(anti_or_semi_match_sel),
        with_nl_param_(with_nl_param),
        need_mat_(need_mat)
  {}
  virtual ~ObCostNLJoinInfo()
  {}
  TO_STRING_KV(K_(left_rows), K_(left_cost), K_(right_rows), K_(right_cost), K_(left_width), K_(right_width),
      K_(left_ids), K_(right_ids), K_(join_type), K_(anti_or_semi_match_sel), K_(with_nl_param), K_(need_mat),
      K_(equal_join_condition), K_(other_join_condition));
  double anti_or_semi_match_sel_;
  bool with_nl_param_;
  bool need_mat_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCostNLJoinInfo);
};

struct ObCostMergeJoinInfo : public ObCostBaseJoinInfo {
  ObCostMergeJoinInfo(double left_rows, double left_cost, double left_width, double right_rows, double right_cost,
      double right_width, ObRelIds left_ids, ObRelIds right_ids, ObJoinType join_type,
      const common::ObIArray<ObRawExpr*>& equal_join_condition,
      const common::ObIArray<ObRawExpr*>& other_join_condition, const common::ObIArray<OrderItem>& left_need_ordering,
      const common::ObIArray<OrderItem>& right_need_ordering, ObEstSelInfo* est_sel_info)
      : ObCostBaseJoinInfo(left_rows, left_cost, left_width, right_rows, right_cost, right_width, left_ids, right_ids,
            join_type, equal_join_condition, other_join_condition, est_sel_info),
        left_need_ordering_(left_need_ordering),
        right_need_ordering_(right_need_ordering)
  {}
  virtual ~ObCostMergeJoinInfo(){};
  TO_STRING_KV(K_(left_rows), K_(left_cost), K_(right_rows), K_(right_cost), K_(left_width), K_(right_width),
      K_(left_ids), K_(right_ids), K_(join_type), K_(left_need_ordering), K_(right_need_ordering),
      K_(equal_join_condition), K_(other_join_condition));
  const common::ObIArray<OrderItem>& left_need_ordering_;
  const common::ObIArray<OrderItem>& right_need_ordering_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCostMergeJoinInfo);
};

/**
 * so far hash join info only contain variables from base class,
 * in future, we will change cost model and may need additional information
 */
struct ObCostHashJoinInfo : public ObCostBaseJoinInfo {
  ObCostHashJoinInfo(double left_rows, double left_cost, double left_width, double right_rows, double right_cost,
      double right_width, ObRelIds left_ids, ObRelIds right_ids, ObJoinType join_type,
      const common::ObIArray<ObRawExpr*>& equal_join_condition,
      const common::ObIArray<ObRawExpr*>& other_join_condition, ObEstSelInfo* est_sel_info)
      : ObCostBaseJoinInfo(left_rows, left_cost, left_width, right_rows, right_cost, right_width, left_ids, right_ids,
            join_type, equal_join_condition, other_join_condition, est_sel_info){};
  TO_STRING_KV(K_(left_rows), K_(left_cost), K_(right_rows), K_(right_cost), K_(left_width), K_(right_width),
      K_(left_ids), K_(right_ids), K_(join_type), K_(equal_join_condition), K_(other_join_condition));
  virtual ~ObCostHashJoinInfo(){};

private:
  DISALLOW_COPY_AND_ASSIGN(ObCostHashJoinInfo);
};

struct ObSubplanFilterCostInfo {
  ObSubplanFilterCostInfo(const ObIArray<ObBasicCostInfo>& children,
      const ObIArray<std::pair<int64_t, ObRawExpr*> >& onetime_exprs, const ObBitSet<>& initplans)
      : children_(children), onetime_exprs_(onetime_exprs), initplans_(initplans)
  {}
  TO_STRING_KV(K_(children), K_(onetime_exprs), K_(initplans));

  const ObIArray<ObBasicCostInfo>& children_;
  const ObIArray<std::pair<int64_t, ObRawExpr*> >& onetime_exprs_;
  const ObBitSet<>& initplans_;
};

struct ObCostMergeSetInfo {
  ObCostMergeSetInfo(const ObIArray<ObBasicCostInfo>& children, int64_t op, int64_t num_select_items)
      : children_(children), op_(op), num_select_items_(num_select_items)
  {}

  const ObIArray<ObBasicCostInfo>& children_;
  int64_t op_;
  int64_t num_select_items_;
};

struct ObCostHashSetInfo : public ObTwoChildrenNodeCostInfo {
  ObCostHashSetInfo(double left_rows, double left_cost, double left_width, double right_rows, double right_cost,
      double right_width, int64_t op, const ObIArray<ObRawExpr*>& hash_columns)
      : ObTwoChildrenNodeCostInfo(left_rows, left_cost, left_width, right_rows, right_cost, right_width),
        op_(op),
        hash_columns_(hash_columns)
  {}

  int64_t op_;
  const ObIArray<ObRawExpr*>& hash_columns_;
};

struct ObSortCostInfo {
  ObSortCostInfo(double rows, double width, int64_t prefix_pos, ObEstSelInfo* est_sel_info, double topn = -1)
      : rows_(rows), width_(width), prefix_pos_(prefix_pos), est_sel_info_(est_sel_info), topn_(topn)
  {}

  const ObEstSelInfo* get_est_sel_info() const
  {
    return est_sel_info_;
  }

  double rows_;
  double width_;
  // not prefix sort if prefix_pos_ <= 0
  int64_t prefix_pos_;
  // used to calculate ndv in prefix sort
  const ObEstSelInfo* est_sel_info_;
  // not topn sort if topn_ < 0
  double topn_;
};

class ObOptEstCost {
public:
  const static int64_t MAX_STORAGE_RANGE_ESTIMATION_NUM;

  struct ObCostParams {
    explicit ObCostParams()
        : CPU_TUPLE_COST(DEFAULT_CPU_TUPLE_COST),
          MICRO_BLOCK_SEQ_COST(DEFAULT_MICRO_BLOCK_SEQ_COST),
          MICRO_BLOCK_RND_COST(DEFAULT_MICRO_BLOCK_RND_COST),
          PROJECT_COLUMN_SEQ_COST(DEFAULT_PROJECT_COLUMN_SEQ_COST),
          PROJECT_COLUMN_RND_COST(DEFAULT_PROJECT_COLUMN_RND_COST),
          FETCH_ROW_RND_COST(DEFAULT_FETCH_ROW_RND_COST),
          CMP_DEFAULT_COST(DEFAULT_CMP_INT_COST),
          CMP_INT_COST(DEFAULT_CMP_INT_COST),
          CMP_NUMBER_COST(DEFAULT_CMP_NUMBER_COST),
          CMP_CHAR_COST(DEFAULT_CMP_CHAR_COST),
          HASH_DEFAULT_COST(DEFAULT_HASH_INT_COST),
          HASH_INT_COST(DEFAULT_HASH_INT_COST),
          HASH_NUMBER_COST(DEFAULT_HASH_NUMBER_COST),
          HASH_CHAR_COST(DEFAULT_HASH_CHAR_COST),
          MATERIALIZE_PER_BYTE_COST(DEFAULT_MATERIALIZE_PER_BYTE_COST),
          MATERIALIZED_ROW_COST(DEFAULT_MATERIALIZED_ROW_COST),
          MATERIALIZED_BYTE_READ_COST(DEFAULT_MATERIALIZED_BYTE_READ_COST),
          PER_AGGR_FUNC_COST(DEFAULT_PER_AGGR_FUNC_COST),
          CPU_OPERATOR_COST(DEFAULT_CPU_OPERATOR_COST),
          JOIN_PER_ROW_COST(DEFAULT_JOIN_PER_ROW_COST),
          BUILD_HASH_PER_ROW_COST(DEFAULT_BUILD_HASH_PER_ROW_COST),
          PROBE_HASH_PER_ROW_COST(DEFAULT_PROBE_HASH_PER_ROW_COST),
          NETWORK_PER_BYTE_COST(DEFAULT_NETWORK_PER_BYTE_COST),
          MAX_STRING_WIDTH(DEFAULT_MAX_STRING_WIDTH)
    {}
    double CPU_TUPLE_COST;
    double MICRO_BLOCK_SEQ_COST;
    double MICRO_BLOCK_RND_COST;
    double PROJECT_COLUMN_SEQ_COST;
    double PROJECT_COLUMN_RND_COST;
    double FETCH_ROW_RND_COST;
    double CMP_DEFAULT_COST;
    double CMP_INT_COST;
    double CMP_NUMBER_COST;
    double CMP_CHAR_COST;
    double HASH_DEFAULT_COST;
    double HASH_INT_COST;
    double HASH_NUMBER_COST;
    double HASH_CHAR_COST;
    double MATERIALIZE_PER_BYTE_COST;
    double MATERIALIZED_ROW_COST;
    double MATERIALIZED_BYTE_READ_COST;
    double PER_AGGR_FUNC_COST;
    double CPU_OPERATOR_COST;
    double JOIN_PER_ROW_COST;
    double BUILD_HASH_PER_ROW_COST;
    double PROBE_HASH_PER_ROW_COST;
    double NETWORK_PER_BYTE_COST;
    int64_t MAX_STRING_WIDTH;

    static const double DEFAULT_CPU_TUPLE_COST;
    static const double DEFAULT_MICRO_BLOCK_SEQ_COST;
    static const double DEFAULT_MICRO_BLOCK_RND_COST;
    static const double DEFAULT_PROJECT_COLUMN_SEQ_COST;
    static const double DEFAULT_PROJECT_COLUMN_RND_COST;
    static const double DEFAULT_FETCH_ROW_RND_COST;
    static const double DEFAULT_CMP_INT_COST;
    static const double DEFAULT_CMP_NUMBER_COST;
    static const double DEFAULT_CMP_CHAR_COST;
    static const double INVALID_CMP_COST;
    static const double DEFAULT_HASH_INT_COST;
    static const double DEFAULT_HASH_NUMBER_COST;
    static const double DEFAULT_HASH_CHAR_COST;
    static const double INVALID_HASH_COST;
    static const double DEFAULT_MATERIALIZED_BYTE_READ_COST;
    static const double DEFAULT_MATERIALIZED_ROW_COST;
    static const double DEFAULT_MATERIALIZE_PER_BYTE_COST;
    static const double DEFAULT_PER_AGGR_FUNC_COST;
    static const double DEFAULT_CPU_OPERATOR_COST;
    static const double DEFAULT_JOIN_PER_ROW_COST;
    static const double DEFAULT_BUILD_HASH_PER_ROW_COST;
    static const double DEFAULT_PROBE_HASH_PER_ROW_COST;
    static const double DEFAULT_NETWORK_PER_BYTE_COST;
    static const int64_t DEFAULT_MAX_STRING_WIDTH = 64;
    static const int64_t DEFAULT_NET_BASE_COST = 100;
    static const double DEFAULT_NET_PER_ROW_COST;
  };

  static const ObCostParams& get_cost_params()
  {
    return cost_params_;
  }

  static int extract_rel_vars(const ObDMLStmt* stmt, const ObRelIds& relids, const common::ObIArray<ObRawExpr*>& exprs,
      common::ObIArray<ObRawExpr*>& vars);
  static int extract_rel_vars(
      const ObDMLStmt* stmt, const ObRelIds& relids, const ObRawExpr* expr, common::ObIArray<ObRawExpr*>& vars);

  static int cost_nestloop(const ObCostNLJoinInfo& est_cost_info, double& op_cost, double& nl_cost,
      common::ObIArray<ObExprSelPair>* all_predicate_sel);

  static int cost_mergejoin(const ObCostMergeJoinInfo& est_cost_info, double& op_cost, double& mj_cost,
      common::ObIArray<ObExprSelPair>* all_predicate_sel);

  static int cost_hashjoin(const ObCostHashJoinInfo& est_cost_info, double& op_cost, double& hash_cost,
      common::ObIArray<ObExprSelPair>* all_predicate_sel);

  static int cost_sort(const ObSortCostInfo& cost_info, const common::ObIArray<ObRawExpr*>& order_exprs, double& cost);
  static int cost_sort(const ObSortCostInfo& cost_info, const common::ObIArray<OrderItem>& order_items, double& cost);

  static int cost_sort(
      const ObSortCostInfo& cost_info, const common::ObIArray<ObExprResType>& order_col_types, double& cost);

  static double cost_merge_group(double rows, double res_rows, const ObIArray<ObRawExpr*>& group_columns,
      int64_t agg_col_count, int64_t input_col_count = 10);

  static double cost_hash_group(
      double rows, double res_rows, const ObIArray<ObRawExpr*>& group_columns, int64_t agg_col_count);

  static double cost_scalar_group(double rows, int64_t agg_col_count);

  static double cost_merge_distinct(
      const double rows, const double res_rows, const ObIArray<ObRawExpr*>& distinct_columns);

  static double cost_hash_distinct(double rows, double res_rows, const ObIArray<ObRawExpr*>& disinct_columns);

  static double cost_limit(double rows);
  static double cost_sequence(double rows, double uniq_sequence_cnt);
  static double cost_material(const double rows, const double average_row_size = 8);
  static double cost_read_materialized(const double rows, const double average_row_size = 8);
  static double cost_transmit(const double rows, const double average_row_size = 8);
  static double cost_subplan_scan(double rows, int64_t num_filters);
  static int cost_subplan_filter(const ObSubplanFilterCostInfo& info, double& op_cost, double& cost);
  static int is_subquery_one_time(const ObRawExpr* expr, int64_t query_id, bool& is_onetime_expr);
  static int cost_merge_set(const ObCostMergeSetInfo& info, double& rows, double& op_cost, double& cost);
  static int cost_hash_set(const ObCostHashSetInfo& info, double& rows, double& op_cost, double& cost);

  static double cost_quals(double rows, int64_t num_quals);
  static double cost_quals(double rows, const ObIArray<ObRawExpr*>& quals);
  static double cost_hash(double rows, int64_t num_hash_columns);
  static double cost_hash(double rows, const ObIArray<ObRawExpr*>& hash_exprs);
  static int64_t revise_ge_1(int64_t num);
  static double revise_ge_1(double num);
  static int64_t revise_by_sel(int64_t num, double sel);

  static double cost_count(double rows);

  static double cost_temp_table(double rows, double width, const ObIArray<ObRawExpr*>& filters);
  /*
   * entry point for estimating table access cost
   */
  static int cost_table(ObCostTableScanInfo& est_cost_info, double& query_range_row_count,
      double& phy_query_range_row_count, double& cost, double& index_back_cost);

  // estimate one batch
  static int cost_table_one_batch(const ObCostTableScanInfo& est_cost_info,
      const common::ObSimpleBatch::ObBatchType& type, const bool is_index_back, const int64_t column_count,
      const double logical_output_row_count, const double physical_output_row_count, double& cost,
      double& index_back_cost);

  // estimate one batch table scan cost
  static int cost_table_scan_one_batch(const ObCostTableScanInfo& est_cost_info, const bool is_index_back,
      const int64_t column_count, const double logical_output_row_count, const double physical_output_row_count,
      double& cost, double& index_back_cost);

  // estimate one batch table get cost
  static int cost_table_get_one_batch(const ObCostTableScanInfo& est_cost_info, const bool is_index_back,
      const int64_t column_count, const double output_row_count, double& cost, double& index_back_cost);

  static double cost_late_materialization_table_get(int64_t column_cnt);

  static void cost_late_materialization_table_join(
      double left_card, double left_cost, double right_card, double right_cost, double& op_cost, double& cost);

  static int get_sort_cmp_cost(const common::ObIArray<sql::ObExprResType>& types, double& cost);
  static int cost_window_function(double rows, double& cost);
  static int estimate_width_for_table(const ObIArray<ColumnItem>& columns, int64_t table_id, double& width);
  static int estimate_width_for_columns(const ObIArray<ObRawExpr*>& columns, double& width);

  // mainly for acs plan to estimate selectivity
  static int estimate_acs_partition_rowcount(const storage::ObTableScanParam& table_scan_param,
      storage::ObPartitionService* partition_service, double& row_count);

  static int construct_scan_range_batch(const ObIArray<ObNewRange>& scan_ranges, common::ObSimpleBatch& batch,
      common::SQLScanRange& range, common::SQLScanRangeArray& range_array);

  /**
   * @brief storage_estimate_rowcount
   * do storage rowcount estimation for multiple indexes
   */
  static int storage_estimate_rowcount(const storage::ObPartitionService* part_service, ObStatManager& stat_manager,
      const obrpc::ObEstPartArg& arg, obrpc::ObEstPartRes& res);

  /**
   * @brief storage_estimate_rowcount
   * estimate rowcount for an index access path using storage interface
   */
  static int storage_estimate_rowcount(const storage::ObPartitionService* part_service,
      const storage::ObTableScanParam& param, const ObSimpleBatch& batch, const int64_t range_columns_count,
      obrpc::ObEstPartResElement& res);

  static int compute_partition_rowcount_and_size(const obrpc::ObEstPartArg& arg, ObStatManager& stat_manager,
      const storage::ObPartitionService* part_service, int64_t& logical_row_count, int64_t& part_size,
      double& avg_row_size);

  // compute memtable row count
  static int estimate_memtable_row_count(const storage::ObTableScanParam& param,
      const storage::ObPartitionService* part_service, const int64_t column_count, int64_t& logical_row_count,
      int64_t& physical_row_count);

  static int storage_estimate_partition_batch_rowcount(const ObPartitionKey& pkey, const common::ObSimpleBatch& batch,
      const storage::ObTableScanParam& table_scan_param, const int64_t range_columns_count,
      const storage::ObPartitionService* part_service, ObIArray<ObEstRowCountRecord>& est_records,
      double& logical_row_count, double& physical_row_count);

  static int stat_estimate_partition_batch_rowcount(
      const ObCostTableScanInfo& est_cost_info, const ObIArray<ObNewRange>& scan_ranges, double& row_count);

  static int get_sstable_rowcount_and_size(ObStatManager& stat_manager, const ObIArray<ObPartitionKey>& part_keys,
      int64_t& row_count, int64_t& part_size, double& avg_row_size);

  static int estimate_row_count(const obrpc::ObEstPartResElement& result, ObCostTableScanInfo& est_cost_info,
      ObIArray<ObExprSelPair>* all_predicate_sel, double& output_row_count, double& query_range_row_count,
      double& phy_query_range_row_count, double& index_back_row_count);

private:
  static int cost_prefix_sort(const ObSortCostInfo& cost_info, const ObIArray<ObRawExpr*>& order_exprs, double& cost);
  static int cost_topn_sort(const ObSortCostInfo& cost_info, const ObIArray<ObExprResType>& types, double& cost);
  static int cost_topn_sort_inner(const ObIArray<ObExprResType>& types, double rows, double n, double& cost);
  // calculate real sort cost (std::sort)
  static int cost_sort_inner(const common::ObIArray<sql::ObExprResType>& types, double row_count, double& cost);

  // estimate cost for virtual table
  static int cost_virtual_table(ObCostTableScanInfo& est_cost_info, double& cost);

  // estimate cost for non-virtual table
  static int cost_normal_table(ObCostTableScanInfo& est_cost_info, const double query_range_row_count,
      const double phy_query_range_row_count, double& cost, double& index_back_cost);

  // estimate the network transform and rpc cost for global index
  static int cost_table_get_one_batch_network(
      double row_count, double column_count, const ObCostTableScanInfo& est_cost_info, double& cost);

  static int cost_table_get_one_batch_inner(
      double row_count, double column_count, const ObCostTableScanInfo& est_cost_info, bool is_index_back, double& res);

  // estimate one batch table scan cost, truly estimation function
  static int cost_table_scan_one_batch_inner(double row_count, double column_count, double block_cache_rate,
      const ObCostTableScanInfo& est_cost_info, bool is_index_back, double& res);

  static int calculate_filter_selectivity(
      ObCostTableScanInfo& est_cost_info, common::ObIArray<ObExprSelPair>* all_predicate_sel);

  static int estimate_partition_range_rowcount(const obrpc::ObEstPartResElement& result,
      ObCostTableScanInfo& est_cost_info, double& logical_row_count, double& physical_row_count);

  static int estimate_partition_scan_batch_rowcount(ObCostTableScanInfo& est_cost_info,
      const ObIArray<ObNewRange>& scan_ranges, const obrpc::ObEstPartResElement& result, double& scan_logical_rc,
      double& scan_physical_rc);

  static int get_normal_replica(const ObPartitionKey& pkey, const storage::ObPartitionService* part_service,
      storage::ObIPartitionGroupGuard& guard);

  static int stat_estimate_single_range_rc(
      const ObCostTableScanInfo& est_cost_info, const ObNewRange& range, double& count);

  static double calc_single_value_exceeds_limit_selectivity(double min, double max, double value, double base_sel,
      double zero_sel_range_ratio = common::DEFAULT_RANGE_EXCEED_LIMIT_DECAY_RATIO);

  enum TYPE_RELATED_PARAMS_TYPE {
    ROWSTORE_READ_BASIC = 0,
    ROWSTORE_WRITE_BASIC,
    SORT_CMP_COST,
    TYPE_RELATED_PARAMS_TYPE_CNT
  };

  const static double comparison_params_[common::ObMaxTC + 1];
  const static double hash_params_[common::ObMaxTC + 1];
  const static ObCostParams cost_params_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObOptEstCost);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_COST_ */
