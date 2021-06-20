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

#ifndef _OB_JOIN_ORDER_H
#define _OB_JOIN_ORDER_H 1
#include "lib/container/ob_int_flags.h"
#include "sql/ob_sql_define.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_index_info_cache.h"
#include "sql/optimizer/ob_opt_est_sel.h"
#include "lib/container/ob_bit_set.h"
#include "sql/optimizer/ob_fd_item.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_plan.h"

using oceanbase::common::ObString;
namespace test {
class TestJoinOrder_ob_join_order_param_check_Test;
class TestJoinOrder_ob_join_order_src_Test;
}  // namespace test

namespace oceanbase {
namespace obrpc {
class ObSrvRpcProxy;
}
namespace share {
namespace schema {
class ObSchemaGetterGuard;
}
}  // namespace share
namespace sql {
class ObJoinOrder;
class ObIndexSkylineDim;
class ObIndexInfoCache;
class ObSelectLogPlan;

struct JoinInfo {
  JoinInfo() : table_set_(), on_condition_(), where_condition_(), equal_join_condition_(), join_type_(UNKNOWN_JOIN)
  {}

  JoinInfo(ObJoinType join_type)
      : table_set_(), on_condition_(), where_condition_(), equal_join_condition_(), join_type_(join_type)
  {}

  virtual ~JoinInfo(){};
  TO_STRING_KV(K_(join_type), K_(table_set), K_(on_condition), K_(where_condition), K_(equal_join_condition));
  ObRelIds table_set_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> on_condition_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> where_condition_;
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> equal_join_condition_;
  ObJoinType join_type_;
};

struct ConflictDetector {
  ConflictDetector()
      : join_info_(),
        CR_(),
        cross_product_rule_(),
        delay_cross_product_rule_(),
        L_TES_(),
        R_TES_(),
        L_DS_(),
        R_DS_(),
        is_degenerate_pred_(false),
        is_commutative_(false),
        is_redundancy_(false)
  {}

  virtual ~ConflictDetector()
  {}

  static int build_confict(common::ObIAllocator& allocator, ConflictDetector*& detector);

  TO_STRING_KV(K_(join_info), K_(CR), K_(cross_product_rule), K_(delay_cross_product_rule), K_(L_TES), K_(R_TES),
      K_(L_DS), K_(R_DS), K_(is_degenerate_pred), K_(is_commutative), K_(is_redundancy));

  JoinInfo join_info_;
  // conflict rules: R1 -> R2
  common::ObSEArray<std::pair<ObRelIds, ObRelIds>, 4, common::ModulePageAllocator, true> CR_;
  common::ObSEArray<std::pair<ObRelIds, ObRelIds>, 4, common::ModulePageAllocator, true> cross_product_rule_;
  common::ObSEArray<std::pair<ObRelIds, ObRelIds>, 4, common::ModulePageAllocator, true> delay_cross_product_rule_;
  // left total eligibility set
  ObRelIds L_TES_;
  // right total eligibility set
  ObRelIds R_TES_;
  ObRelIds L_DS_;
  ObRelIds R_DS_;
  bool is_degenerate_pred_;
  bool is_commutative_;
  bool is_redundancy_;
};

enum CompareRelation { LEFT_DOMINATE, RIGHT_DOMINATE, EQUAL, UNCOMPARABLE };

enum OptimizationMethod { RULE_BASED = 0, COST_BASED, MAX_METHOD };

enum HeuristicRule {
  UNIQUE_INDEX_WITHOUT_INDEXBACK = 0,
  UNIQUE_INDEX_WITH_INDEXBACK,
  VIRTUAL_TABLE_HEURISTIC,  // only for virtual table
  MAX_RULE
};

struct BaseTableOptInfo {
  BaseTableOptInfo()
      : optimization_method_(OptimizationMethod::MAX_METHOD),
        heuristic_rule_(HeuristicRule::MAX_RULE),
        available_index_id_(),
        available_index_name_(),
        pruned_index_name_()
  {}

  // this following variables are tracked to remember how base table access path are generated
  OptimizationMethod optimization_method_;
  HeuristicRule heuristic_rule_;
  common::ObSEArray<uint64_t, 4, common::ModulePageAllocator, true> available_index_id_;
  common::ObSEArray<common::ObString, 4, common::ModulePageAllocator, true> available_index_name_;
  common::ObSEArray<common::ObString, 4, common::ModulePageAllocator, true> pruned_index_name_;
};

class Path {
public:
  Path()
      : path_type_(INVALID),
        parent_(NULL),
        ordering_(),
        filter_(),
        cost_(0.0),
        op_cost_(0.0),
        exec_params_(),
        log_op_(NULL),
        interesting_order_info_(OrderingFlag::NOT_MATCH),
        is_inner_path_(false),
        inner_row_count_(0),
        pushdown_filters_(),
        nl_params_()
  {}
  Path(PathType path_type, ObJoinOrder* parent)
      : path_type_(path_type),
        parent_(parent),
        ordering_(),
        filter_(),
        cost_(0.0),
        op_cost_(0.0),
        exec_params_(),
        log_op_(NULL),
        interesting_order_info_(OrderingFlag::NOT_MATCH),
        is_inner_path_(false),
        inner_row_count_(0),
        pushdown_filters_(),
        nl_params_()
  {}
  virtual ~Path()
  {}
  int deep_copy(const Path& other, common::ObIAllocator* allocator);
  int add_sortkey(OrderItem& key)
  {
    return ordering_.push_back(key);
  }
  int add_sortkeys(common::ObIArray<OrderItem>& key_array)
  {
    return append(ordering_, key_array);
  }
  int set_ordering(const common::ObIArray<ObRawExpr*>& ordering, const ObOrderDirection& direction)
  {
    int ret = common::OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < ordering.count(); ++i) {
      ret = ordering_.push_back(OrderItem(ordering.at(i), direction));
    }
    return ret;
  }

  inline const common::ObIArray<OrderItem>& get_ordering() const
  {
    return ordering_;
  }
  inline common::ObIArray<OrderItem>& get_ordering()
  {
    return ordering_;
  }

  inline int get_interesting_order_info() const
  {
    return interesting_order_info_;
  }
  inline void set_interesting_order_info(int64_t info)
  {
    interesting_order_info_ = info;
  }
  inline void add_interesting_order_flag(OrderingFlag flag)
  {
    interesting_order_info_ |= flag;
  }
  inline void add_interesting_order_flag(int64_t flags)
  {
    interesting_order_info_ |= flags;
  }
  inline void clear_interesting_order_flag(OrderingFlag flag)
  {
    interesting_order_info_ &= ~flag;
  }
  inline bool has_interesting_order_flag(OrderingFlag flag) const
  {
    return (interesting_order_info_ & flag) > 0;
  }
  inline bool has_interesting_order() const
  {
    return interesting_order_info_ > 0;
  }
  bool is_inner_path() const
  {
    return is_inner_path_;
  }
  void set_is_inner_path(bool is)
  {
    is_inner_path_ = is;
  }
  double get_cost() const
  {
    return cost_;
  }
  virtual void get_name_internal(char* buf, const int64_t buf_len, int64_t& pos) const = 0;
  int get_name(char* buf, const int64_t buf_len, int64_t& pos)
  {
    int ret = common::OB_SUCCESS;
    pos = 0;
    get_name_internal(buf, buf_len, pos);
    common::ObIArray<OrderItem>& ordering = ordering_;
    if (OB_FAIL(BUF_PRINTF("("))) { /* Do nothing */
    } else {
      EXPLAIN_PRINT_SORT_KEYS(ordering, EXPLAIN_UNINITIALIZED);
    }
    if (OB_FAIL(ret)) {                                 /* Do nothing */
    } else if (OB_FAIL(BUF_PRINTF(", "))) {             /* Do nothing */
    } else if (OB_FAIL(BUF_PRINTF("cost=%f", cost_))) { /* Do nothing */
    } else {
      ret = BUF_PRINTF(")");
    }
    return ret;
  }
  bool is_valid() const;
  virtual int estimate_cost() = 0;
  TO_STRING_KV(K_(path_type), K_(cost), K_(op_cost), K_(exec_params), K_(ordering), K_(is_inner_path),
      K_(inner_row_count), K_(interesting_order_info));

public:
  // member variables
  PathType path_type_;
  ObJoinOrder* parent_;
  common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> ordering_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> filter_;
  double cost_;
  double op_cost_;
  common::ObSEArray<std::pair<int64_t, ObRawExpr*>, 16, common::ModulePageAllocator, true> exec_params_;
  ObLogicalOperator* log_op_;
  int64_t interesting_order_info_;
  bool is_inner_path_;      // inner path with push down filters
  double inner_row_count_;  // inner path output row count
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true>
      pushdown_filters_;  // original push down filters(without being replaced by ?) for inner path
  common::ObSEArray<std::pair<int64_t, ObRawExpr*>, 4, common::ModulePageAllocator, true> nl_params_;  // parameters for
                                                                                                       // inner path

private:
  DISALLOW_COPY_AND_ASSIGN(Path);
};

class AccessPath : public Path {
public:
  AccessPath(
      uint64_t table_id, uint64_t ref_table_id, uint64_t index_id, ObJoinOrder* parent, ObOrderDirection direction)
      : Path(ACCESS, parent),
        table_id_(table_id),
        ref_table_id_(ref_table_id),
        index_id_(index_id),
        is_global_index_(false),
        table_partition_info_(NULL),
        sharding_info_(NULL),
        is_cte_table_path_(false),
        is_function_table_path_(false),
        is_temp_table_path_(false),
        index_keys_(),
        pre_query_range_(NULL),
        is_get_(false),
        order_direction_(direction),
        is_hash_index_(false),
        table_row_count_(0),
        output_row_count_(0.0),
        phy_query_range_row_count_(0),
        query_range_row_count_(0),
        index_back_row_count_(0),
        index_back_cost_(0.0),
        est_cost_info_(table_id, ref_table_id, index_id),
        est_records_(),
        range_prefix_count_(0),
        table_opt_info_()
  {}
  virtual ~AccessPath()
  {}
  int deep_copy(const AccessPath& other, common::ObIAllocator* allocator);
  uint64_t get_table_id() const
  {
    return table_id_;
  }
  void set_table_id(uint64_t table_id)
  {
    table_id_ = table_id;
  }
  uint64_t get_ref_table_id() const
  {
    return ref_table_id_;
  }
  void set_ref_table_id(uint64_t ref_id)
  {
    ref_table_id_ = ref_id;
  }
  uint64_t get_index_table_id() const
  {
    return index_id_;
  }
  void set_index_table_id(uint64_t index_id)
  {
    index_id_ = index_id;
  }
  bool is_get() const
  {
    return is_get_;
  }
  void set_is_get(bool is_get)
  {
    is_get_ = is_get;
  }
  void set_output_row_count(double output_row_count)
  {
    output_row_count_ = output_row_count;
  }
  void set_table_row_count(int64_t table_row_count)
  {
    table_row_count_ = table_row_count;
  }
  int64_t get_table_row_count() const
  {
    return table_row_count_;
  }
  double get_output_row_count() const
  {
    return output_row_count_;
  }
  bool is_cte_path() const
  {
    return is_cte_table_path_;
  }
  void set_cte_path(bool is_cte_table_path)
  {
    is_cte_table_path_ = is_cte_table_path;
  }
  bool is_function_table_path() const
  {
    return is_function_table_path_;
  }
  void set_function_table_path(bool is_function_table_path)
  {
    is_function_table_path_ = is_function_table_path;
  }
  bool is_temp_table_path() const
  {
    return is_temp_table_path_;
  }
  void set_temp_table_path(bool is_temp_table_path)
  {
    is_temp_table_path_ = is_temp_table_path;
  }
  double get_cost()
  {
    return cost_;
  }
  const ObCostTableScanInfo& get_cost_table_scan_info() const
  {
    return est_cost_info_;
  }
  ObCostTableScanInfo& get_cost_table_scan_info()
  {
    return est_cost_info_;
  }
  const ObShardingInfo* get_sharding_info() const;
  virtual void get_name_internal(char* buf, const int64_t buf_len, int64_t& pos) const
  {
    BUF_PRINTF("@");
    BUF_PRINTF("%lu", table_id_);
  }
  virtual int estimate_cost() override;

  TO_STRING_KV(K_(table_id), K_(ref_table_id), K_(index_id), K_(is_cte_table_path), K_(path_type), K_(cost),
      K_(exec_params), K_(ordering), K_(is_get), K_(order_direction), K_(is_hash_index), K_(table_row_count),
      K_(output_row_count), K_(phy_query_range_row_count), K_(query_range_row_count), K_(index_back_row_count),
      K_(index_back_cost), K_(est_cost_info), K_(sample_info), K_(range_prefix_count));

public:
  // member variables
  uint64_t table_id_;
  uint64_t ref_table_id_;
  uint64_t index_id_;
  bool is_global_index_;
  ObTablePartitionInfo* table_partition_info_;  // for global index
  ObShardingInfo* sharding_info_;               // for global index
  bool is_cte_table_path_;
  bool is_function_table_path_;
  bool is_temp_table_path_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> index_keys_;  // index keys
  ObQueryRange* pre_query_range_;  // pre_query_range for each access path
  bool is_get_;
  ObOrderDirection order_direction_;
  bool is_hash_index_;       // is hash index (virtual table and is index)
  int64_t table_row_count_;  // all table row count
  double output_row_count_;
  double phy_query_range_row_count_;
  double query_range_row_count_;
  double index_back_row_count_;
  double index_back_cost_;             // cost for index back
  ObCostTableScanInfo est_cost_info_;  // estimate cost info
  common::ObSEArray<ObEstRowCountRecord, MAX_SSTABLE_CNT_IN_STORAGE, common::ModulePageAllocator, true> est_records_;
  SampleInfo sample_info_;      // sample scan info
  int64_t range_prefix_count_;  // prefix count
  BaseTableOptInfo* table_opt_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(AccessPath);
};

class JoinPath : public Path {
public:
  JoinPath()
      : Path(JOIN, NULL),
        left_path_(NULL),
        right_path_(NULL),
        join_algo_(INVALID_JOIN_ALGO),
        join_type_(UNKNOWN_JOIN),
        need_mat_(false),
        left_need_sort_(false),
        right_need_sort_(false),
        left_expected_ordering_(),
        right_expected_ordering_(),
        merge_directions_(),
        equal_join_condition_(),
        other_join_condition_()
  {}

  JoinPath(ObJoinOrder* parent, const Path* left_path, const Path* right_path, JoinAlgo join_algo, ObJoinType join_type,
      bool need_mat = false)
      : Path(JOIN, parent),
        left_path_(left_path),
        right_path_(right_path),
        join_algo_(join_algo),
        join_type_(join_type),
        need_mat_(need_mat),
        left_need_sort_(false),
        right_need_sort_(false),
        left_expected_ordering_(),
        right_expected_ordering_(),
        merge_directions_(),
        equal_join_condition_(),
        other_join_condition_()
  {}
  virtual ~JoinPath()
  {}
  virtual int estimate_cost() override;
  int re_est_cost(Path* path, double need_row_count, double& cost);
  int re_est_access_cost(AccessPath* path, double need_row_count, double& cost);
  int re_est_subquery_cost(SubQueryPath* path, double need_row_count, double& cost);
  int cost_nest_loop_join(double& op_cost, double& cost);
  int cost_merge_join(double& op_cost, double& cost);
  int cost_hash_join(double& op_cost, double& cost);
  virtual void get_name_internal(char* buf, const int64_t buf_len, int64_t& pos) const
  {
    BUF_PRINTF("<");
    if (NULL != left_path_) {
      left_path_->get_name_internal(buf, buf_len, pos);
    }
    BUF_PRINTF(" -");

    switch (join_algo_) {
      case INVALID_JOIN_ALGO: {
      } break;
      case NESTED_LOOP_JOIN: {
        BUF_PRINTF("NL");
      } break;
      case MERGE_JOIN: {
        BUF_PRINTF("M");
      } break;
      case HASH_JOIN: {
        BUF_PRINTF("H");
      } break;
      default:
        break;
    }
    BUF_PRINTF("-> ");

    if (NULL != right_path_) {
      right_path_->get_name_internal(buf, buf_len, pos);
    }
    BUF_PRINTF(">");
  }

public:
  const Path* left_path_;
  const Path* right_path_;
  JoinAlgo join_algo_;  // join method
  ObJoinType join_type_;
  bool need_mat_;
  bool left_need_sort_;
  bool right_need_sort_;
  common::ObSEArray<OrderItem, 4, common::ModulePageAllocator, true> left_expected_ordering_;
  common::ObSEArray<OrderItem, 4, common::ModulePageAllocator, true> right_expected_ordering_;
  common::ObSEArray<ObOrderDirection, 4, common::ModulePageAllocator, true> merge_directions_;
  // for all types of join
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> equal_join_condition_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> other_join_condition_;

private:
  DISALLOW_COPY_AND_ASSIGN(JoinPath);
};

class SubQueryPath : public Path {
public:
  SubQueryPath() : Path(SUBQUERY, NULL), subquery_id_(common::OB_INVALID_ID), root_(NULL)
  {}
  SubQueryPath(ObLogicalOperator* root) : Path(SUBQUERY, NULL), subquery_id_(common::OB_INVALID_ID), root_(root)
  {}
  virtual ~SubQueryPath()
  {}
  virtual int estimate_cost() override;
  virtual void get_name_internal(char* buf, const int64_t buf_len, int64_t& pos) const
  {
    BUF_PRINTF("@sub_");
    BUF_PRINTF("%lu", subquery_id_);
  }

public:
  uint64_t subquery_id_;
  ObLogicalOperator* root_;

private:
  DISALLOW_COPY_AND_ASSIGN(SubQueryPath);
};

struct ObRowCountEstTask {
  ObRowCountEstTask() : est_arg_(NULL)
  {}

  ObAddr addr_;
  ObBitSet<> path_id_set_;
  obrpc::ObEstPartArg* est_arg_;

  TO_STRING_KV(K_(addr), K_(path_id_set));
};

struct InnerPathInfo {
  InnerPathInfo() : join_conditions_(), inner_paths_(), table_opt_info_()
  {}
  virtual ~InnerPathInfo()
  {}
  TO_STRING_KV(K_(join_conditions), K_(inner_paths));

  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> join_conditions_;
  common::ObSEArray<Path*, 8, common::ModulePageAllocator, true> inner_paths_;
  BaseTableOptInfo table_opt_info_;
};
typedef common::ObSEArray<InnerPathInfo, 8, common::ModulePageAllocator, true> InnerPathInfos;

class ObJoinOrder {
public:
  struct PathHelper {
    PathHelper()
        : is_inner_path_(false),
          child_stmt_(NULL),
          pushdown_filters_(),
          filters_(),
          inner_paths_(),
          table_opt_info_(NULL)
    {}

    bool is_inner_path_;
    ObSelectStmt* child_stmt_;
    // when generate inner access path, save all pushdown filters
    // when generate subquery path, save all pushdown filters after rename
    common::ObSEArray<ObRawExpr*, 4> pushdown_filters_;
    // when generate inner access path, save base table filters
    // when generate subquery path, save filters can not pushdown
    common::ObSEArray<ObRawExpr*, 4> filters_;
    common::ObSEArray<Path*, 8> inner_paths_;
    BaseTableOptInfo* table_opt_info_;
  };

  // path types
  static const int8_t NEED_NL = 0x1;
  static const int8_t NEED_MJ = 0x1 << 1;
  static const int8_t NEED_HASH = 0x1 << 2;
  static const int8_t NEED_BNL = 0x1 << 3;
  // used for heuristic index selection
  static const int64_t TABLE_HEURISTIC_UNIQUE_KEY_RANGE_THRESHOLD = 10000;

  ObJoinOrder(common::ObIAllocator* allocator, ObLogPlan* plan, PathType type)
      : allocator_(allocator),
        plan_(plan),
        type_(type),
        table_id_(common::OB_INVALID_ID),
        table_set_(),
        output_table_set_(),
        output_rows_(-1.0),
        avg_output_row_size_(-1.0),
        anti_or_semi_match_sel_(0.0),
        table_partition_info_(*allocator_),
        sharding_info_(),
        table_meta_info_(common::OB_INVALID_ID),
        join_info_(NULL),
        used_conflict_detectors_(),
        restrict_info_set_(),
        interesting_paths_(),
        is_init_(false),
        ordering_output_equal_sets_(),
        is_at_most_one_row_(false),
        const_exprs_(),
        table_opt_info_(),
        available_access_paths_(),
        diverse_path_count_(0),
        fd_item_set_(),
        candi_fd_item_set_(),
        not_null_columns_(),
        inner_path_infos_()
  {}
  virtual ~ObJoinOrder();

  int prunning_index(const uint64_t table_id, const uint64_t base_table_id, const ObDMLStmt* stmt,
      const bool do_prunning, const ObIndexInfoCache& index_info_cache,
      const common::ObIArray<uint64_t>& valid_index_ids, common::ObIArray<uint64_t>& skyline_index_ids,
      ObIArray<ObRawExpr*>& restrict_infos);

  int cal_dimension_info(const uint64_t table_id, const uint64_t data_table_id, const uint64_t index_table_id,
      const ObDMLStmt* stmt, ObIndexSkylineDim& index_dim, const ObIndexInfoCache& index_info_cache,
      ObIArray<ObRawExpr*>& restrict_infos);

  int fill_index_info_entry(const uint64_t table_id, const uint64_t base_table_id, const uint64_t index_id,
      IndexInfoEntry*& index_entry, PathHelper& helper);
  int fill_index_info_cache(const uint64_t table_id, const uint64_t base_table_id,
      const common::ObIArray<uint64_t>& valid_index_ids, ObIndexInfoCache& index_info_cache, PathHelper& helper);

  int compute_pruned_index(const uint64_t table_id, const uint64_t base_table_id,
      common::ObIArray<uint64_t>& available_index, PathHelper& helper);

  int extract_used_column_ids(const uint64_t table_id, const uint64_t ref_table_id, ObEstSelInfo& est_sel_info,
      ObIArray<uint64_t>& column_ids, const bool eliminate_rowid_col = false);

  int get_simple_index_info(const uint64_t table_id, const uint64_t ref_table_id, const uint64_t index_id,
      bool& is_unique_index, bool& is_index_back, bool& is_global_index);

  int add_table(
      const uint64_t table_id, const uint64_t ref_table_id, PathHelper& helper, ObIArray<AccessPath*>& access_paths);
  int add_cte_table(const uint64_t data_table_id, const uint64_t ref_id);
  int add_function_table(const uint64_t data_table_id, const uint64_t ref_id);
  inline ObTablePartitionInfo& get_table_partition_info()
  {
    return table_partition_info_;
  }

  int check_multi_stmt_constraint(uint64_t table_id, uint64_t ref_table_id, const ParamStore& param_store,
      const ObIndexInfoCache& index_info_cache, PathHelper& helper, bool& is_vaild);

  int extract_param_for_query_range(const ObIArray<ObRawExpr*>& range_conditions, common::ObIArray<int64_t>& param_pos);
  int extract_param_for_query_range(const ObRawExpr* raw_expr, common::ObIArray<int64_t>& param_pos);

  int add_path(Path* path);
  int compute_path_relationship(const Path* first_path, const Path* second_path, const EqualSets& equal_sets,
      const common::ObIArray<ObRawExpr*>& condition_exprs, const bool has_limit,
      const common::ObIArray<OrderItem>& order_items, CompareRelation& relation);

  int estimate_size_and_width(
      const ObJoinOrder* lefttree, const ObJoinOrder* righttree, const PathType path_type, const ObJoinType join_type);

  int estimate_size_and_width_for_access(PathHelper& helper, ObIArray<AccessPath*>& access_paths);

  int estimate_join_width(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const ObJoinType join_type);

  int compute_equal_set(
      const ObJoinOrder* lefttree, const ObJoinOrder* righttree, const PathType path_type, const ObJoinType join_type);

  int compute_const_exprs(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const PathType path_type,
      const ObJoinType join_type);

  int convert_subplan_scan_order_item(ObRawExprFactory& expr_factory, const EqualSets& equal_sets,
      const ObIArray<ObRawExpr*>& const_exprs, const uint64_t table_id, ObDMLStmt& parent_stmt,
      const ObSelectStmt& child_stmt, const ObIArray<OrderItem>& input_order, ObIArray<OrderItem>& output_order);

  int compute_table_meta_info(const uint64_t table_id, const uint64_t ref_table_id);

  int init_est_sel_info_for_access_path(const uint64_t table_id, const uint64_t ref_table_id);

  int init_est_sel_info_for_subplan_scan(const SubQueryPath* path);

  inline double get_output_rows() const
  {
    return output_rows_;
  }

  inline void set_output_rows(double rows)
  {
    output_rows_ = rows;
  }

  inline common::ObIArray<ConflictDetector*>& get_conflict_detectors()
  {
    return used_conflict_detectors_;
  }
  inline const common::ObIArray<ConflictDetector*>& get_conflict_detectors() const
  {
    return used_conflict_detectors_;
  }
  int merge_conflict_detectors(
      ObJoinOrder* left_tree, ObJoinOrder* right_tree, const common::ObIArray<ConflictDetector*>& detectors);

  inline JoinInfo* get_join_info()
  {
    return join_info_;
  }
  inline const JoinInfo* get_join_info() const
  {
    return join_info_;
  }

  inline ObRelIds& get_tables()
  {
    return table_set_;
  }
  inline const ObRelIds& get_tables() const
  {
    return table_set_;
  }

  inline ObRelIds& get_output_tables()
  {
    return output_table_set_;
  }
  inline const ObRelIds& get_output_tables() const
  {
    return output_table_set_;
  }

  inline common::ObIArray<ObRawExpr*>& get_restrict_infos()
  {
    return restrict_info_set_;
  }

  inline common::ObIArray<Path*>& get_interesting_paths()
  {
    return interesting_paths_;
  }
  inline const common::ObIArray<Path*>& get_interesting_paths() const
  {
    return interesting_paths_;
  }

  inline void set_type(PathType type)
  {
    type_ = type;
  }

  inline PathType get_type()
  {
    return type_;
  }
  inline PathType get_type() const
  {
    return type_;
  }

  inline ObLogPlan* get_plan()
  {
    return plan_;
  }

  inline uint64_t get_table_id() const
  {
    return table_id_;
  }

  const EqualSets& get_ordering_output_equal_sets() const
  {
    return ordering_output_equal_sets_;
  }
  EqualSets& get_ordering_output_equal_sets()
  {
    return ordering_output_equal_sets_;
  }

  inline common::ObIArray<ObRawExpr*>& get_const_exprs()
  {
    return const_exprs_;
  }
  inline const common::ObIArray<ObRawExpr*>& get_const_exprs() const
  {
    return const_exprs_;
  }

  inline void set_is_at_most_one_row(bool is_at_most_one_row)
  {
    is_at_most_one_row_ = is_at_most_one_row;
  }
  inline bool get_is_at_most_one_row() const
  {
    return is_at_most_one_row_;
  }

  int create_access_path(const uint64_t table_id, const uint64_t ref_id, const uint64_t index_id,
      const ObIndexInfoCache& index_info_cache, PathHelper& helper, AccessPath*& ap);

  int fill_cost_table_scan_info(ObCostTableScanInfo& est_cost_info);

  int fill_table_scan_param(ObCostTableScanInfo& est_cost_info);

  int get_access_path_ordering(const uint64_t table_id, const uint64_t ref_table_id, const uint64_t index_id,
      common::ObIArray<ObRawExpr*>& index_keys, common::ObIArray<ObRawExpr*>& ordering, ObOrderDirection& direction);

  int get_index_scan_direction(const ObIArray<ObRawExpr*>& keys, const ObDMLStmt* stmt, const EqualSets& equal_sets,
      ObOrderDirection& index_direction);

  int get_direction_in_order_by(const ObIArray<OrderItem>& order_by, const ObIArray<ObRawExpr*>& index_keys,
      const int64_t index_start_offset, const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
      ObOrderDirection& direction, int64_t& order_match_count);

  /**
   * Extract query range for a certain table(index) given a list of predicates
   */
  int extract_preliminary_query_range(const common::ObIArray<ColumnItem>& range_columns,
      const common::ObIArray<ObRawExpr*>& predicates, ObQueryRange*& range);

  int check_expr_match_first_col(const ObRawExpr* qual, const common::ObIArray<ObRawExpr*>& keys, bool& match);

  int extract_filter_column_ids(const ObIArray<ObRawExpr*>& quals, const bool is_data_table,
      const share::schema::ObTableSchema& index_schema, ObIArray<uint64_t>& filter_column_ids);

  int check_expr_overlap_index(const ObRawExpr* qual, const common::ObIArray<ObRawExpr*>& keys, bool& overlap);

  int check_exprs_overlap_index(
      const common::ObIArray<ObRawExpr*>& quals, const common::ObIArray<ObRawExpr*>& keys, bool& match);

  int is_join_match(const ObIArray<OrderItem>& ordering, int64_t& match_prefix_count, bool& sort_match);

  int check_all_interesting_order(const ObIArray<OrderItem>& ordering, const ObDMLStmt* stmt, int64_t& max_prefix_count,
      int64_t& interesting_order_info);

  int extract_interesting_column_ids(const ObIArray<ObRawExpr*>& keys, const int64_t& max_prefix_count,
      ObIArray<uint64_t>& interest_column_ids, ObIArray<bool>& const_column_info);

  int check_and_extract_filter_column_ids(const ObIArray<ObRawExpr*>& index_keys, ObIArray<uint64_t>& restrict_ids);

  int check_and_extract_query_range(const uint64_t table_id, const uint64_t index_table_id,
      const ObIArray<ObRawExpr*>& index_keys, const ObIndexInfoCache& index_info_cache, bool& contain_always_false,
      common::ObIArray<uint64_t>& prefix_range_ids, ObIArray<ObRawExpr*>& restrict_infos);

  int init_base_join_order(const TableItem* table_item);

  int generate_base_paths();

  int generate_normal_access_path();

  int generate_access_paths(PathHelper& helper);

  int deep_copy_subquery(ObSelectStmt*& stmt);

  int generate_subquery_path(PathHelper& helper);

  int generate_normal_subquery_path();

  int get_range_params(ObLogicalOperator* root, ObIArray<ObRawExpr*>& range_exprs);

  int estimate_size_for_inner_subquery_path(SubQueryPath* path, const ObIArray<ObRawExpr*>& filters);

  int compute_sharding_info_for_paths(const bool is_inner_path, ObIArray<AccessPath*>& access_paths);

  int compute_sharding_info(
      bool can_intra_parallel, ObTablePartitionInfo& table_partition_info, ObShardingInfo& sharding_info);

  int init_join_order(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const JoinInfo* join_info);
  int generate_join_paths(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const JoinInfo* join_info,
      bool force_ordered = false);
  int inner_generate_join_paths(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const ObJoinType join_type,
      const ObJoinType reverse_join_type, const ObIArray<ObRawExpr*>& on_condition,
      const ObIArray<ObRawExpr*>& where_condition, const int8_t path_types, const int8_t reverse_path_types);
  int generate_nl_paths(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const ObJoinType join_type,
      const common::ObIArray<ObRawExpr*>& on_condition, const common::ObIArray<ObRawExpr*>& where_condition,
      const bool has_non_nl_path);
  int generate_hash_paths(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const ObJoinType join_type,
      const common::ObIArray<ObRawExpr*>& equal_join_conditions,
      const common::ObIArray<ObRawExpr*>& other_join_conditions, const common::ObIArray<ObRawExpr*>& filters);
  int generate_mj_paths(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const ObJoinType join_type,
      const common::ObIArray<ObRawExpr*>& equal_join_conditions,
      const common::ObIArray<ObRawExpr*>& other_join_conditions, const common::ObIArray<ObRawExpr*>& filters,
      const bool check_both_side);

  int find_minimal_cost_merge_path(const MergeKeyInfo& left_merge_key, const ObIArray<ObRawExpr*>& right_join_exprs,
      const ObJoinOrder& right_tree, ObIArray<OrderItem>& best_order_items, Path*& best_path, bool& best_need_sort);

  int compute_sort_cost_for_merge_join(Path& path, ObIArray<OrderItem>& expected_ordering, double& cost);

  int init_merge_join_structure(common::ObIAllocator& allocator, const common::ObIArray<Path*>& paths,
      const common::ObIArray<ObRawExpr*>& join_exprs, const common::ObIArray<ObOrderDirection>& join_directions,
      const common::ObIArray<ObRawExpr*>& conditions, common::ObIArray<MergeKeyInfo*>& merge_keys);

  int push_down_order_siblings(JoinPath* join_path, const Path* right_path);
  int create_and_add_nl_path(const Path* left_path, const Path* right_path, const ObJoinType join_type,
      const common::ObIArray<ObRawExpr*>& on_condition, const common::ObIArray<ObRawExpr*>& where_condition,
      bool need_mat = false);

  int create_and_add_hash_path(const Path* left_path, const Path* right_path, const ObJoinType join_type,
      const common::ObIArray<ObRawExpr*>& equal_join_conditions,
      const common::ObIArray<ObRawExpr*>& other_join_conditions, const common::ObIArray<ObRawExpr*>& filters);

  int create_and_add_mj_path(const Path* left_path, const Path* right_path, const ObJoinType join_type,
      const common::ObIArray<ObOrderDirection>& merge_directions,
      const common::ObIArray<ObRawExpr*>& equal_join_conditions,
      const common::ObIArray<ObRawExpr*>& other_join_conditions, const common::ObIArray<ObRawExpr*>& filters,
      const common::ObIArray<OrderItem>& left_sort_keys, const common::ObIArray<OrderItem>& right_sort_keys,
      const bool left_need_sort, const bool right_need_sort);

  int extract_hashjoin_conditions(const common::ObIArray<ObRawExpr*>& join_quals, const ObRelIds& left_tables,
      const ObRelIds& right_tables, common::ObIArray<ObRawExpr*>& equal_join_conditions,
      common::ObIArray<ObRawExpr*>& other_join_conditions);

  int classify_hashjoin_conditions(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree,
      const ObJoinType join_type, const common::ObIArray<ObRawExpr*>& on_condition,
      const common::ObIArray<ObRawExpr*>& where_condition, common::ObIArray<ObRawExpr*>& equal_join_conditions,
      common::ObIArray<ObRawExpr*>& other_join_conditions, common::ObIArray<ObRawExpr*>& filters);

  int classify_mergejoin_conditions(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree,
      const ObJoinType join_type, const common::ObIArray<ObRawExpr*>& on_condition,
      const common::ObIArray<ObRawExpr*>& where_condition, common::ObIArray<ObRawExpr*>& equal_join_conditions,
      common::ObIArray<ObRawExpr*>& other_join_conditions, common::ObIArray<ObRawExpr*>& filters);

  int extract_mergejoin_conditions(const common::ObIArray<ObRawExpr*>& join_quals, const ObRelIds& left_tables,
      const ObRelIds& right_tables, common::ObIArray<ObRawExpr*>& equal_join_conditions,
      common::ObIArray<ObRawExpr*>& other_join_conditions);

  int extract_params_for_inner_path(const uint64_t table_id, const ObRelIds& join_relids,
      ObIArray<std::pair<int64_t, ObRawExpr*>>& nl_params, const ObIArray<ObRawExpr*>& exprs,
      ObIArray<ObRawExpr*>& new_exprs);

  int extract_params_for_inner_path(const uint64_t table_id, const ObRelIds& join_relids,
      ObIArray<std::pair<int64_t, ObRawExpr*>>& nl_params, ObRawExpr* expr, ObRawExpr*& new_expr);

  int extract_params_for_expr(const uint64_t table_id, const ObRelIds& join_relids,
      ObIArray<std::pair<int64_t, ObRawExpr*>>& nl_params, ObRawExpr*& new_expr, ObRawExpr* expr);

  int check_nl_materialization_hint(const ObRelIds& table_ids, bool& force_mat, bool& force_no_mat);

  int check_join_interesting_order(Path* path);

  const ObShardingInfo& get_sharding_info() const
  {
    return sharding_info_;
  }
  ObShardingInfo& get_sharding_info()
  {
    return sharding_info_;
  }

  int64_t get_diverse_path_count() const
  {
    return diverse_path_count_;
  }

  inline double get_anti_or_semi_match_sel() const
  {
    return anti_or_semi_match_sel_;
  }

  const ObFdItemSet& get_fd_item_set() const
  {
    return fd_item_set_;
  }

  ObFdItemSet& get_fd_item_set()
  {
    return fd_item_set_;
  }

  InnerPathInfos& get_inner_path_infos()
  {
    return inner_path_infos_;
  }
  const InnerPathInfos& get_inner_path_infos() const
  {
    return inner_path_infos_;
  }

  int64_t get_name(char* buf, const int64_t buf_len)
  {
    int64_t pos = 0;
    BUF_PRINTF("paths(");
    for (int64_t i = 0; i < interesting_paths_.count(); i++) {
      if (NULL != interesting_paths_.at(0)) {
        int64_t tmp_pos = 0;
        if (FALSE_IT(interesting_paths_.at(0)->get_name(buf + pos, buf_len - pos, tmp_pos))) {
          /* Do nothing */
        } else {
          pos += tmp_pos;
        }
      }
      if (i < interesting_paths_.count() - 1) {
        BUF_PRINTF(", ");
      }
    }
    BUF_PRINTF(")");
    return pos;
  }
  TO_STRING_KV(K_(type), K_(output_rows), K_(interesting_paths));

private:
  int add_access_filters(
      AccessPath* path, const common::ObIArray<ObRawExpr*>& index_keys, const ObIArray<ObRawExpr*>& restrict_infos);

  int set_nl_filters(JoinPath* join_path, const Path* right_path, const ObJoinType join_type,
      const common::ObIArray<ObRawExpr*>& on_condition, const common::ObIArray<ObRawExpr*>& where_condition);

  int fill_query_range_info(const QueryRangeInfo& range_info, ObCostTableScanInfo& est_cost_info);

  int fill_acs_index_info(AccessPath& ap);

  int compute_table_location_for_paths(
      const bool is_inner_path, ObIArray<AccessPath*>& access_paths, ObIArray<ObTablePartitionInfo*>& tbl_part_infos);

  int compute_table_location(const uint64_t table_id, const uint64_t ref_id, ObIArray<AccessPath*>& access_paths,
      const bool is_global_index, ObTablePartitionInfo& table_partition_info);

  inline bool has_array_binding_param(const ParamStore& param_store);

  int get_hint_index_ids(const uint64_t ref_table_id, const ObIndexHint* index_hint, const ObPartHint* part_hint,
      common::ObIArray<uint64_t>& index_ids);

  int get_query_range_info(
      const uint64_t table_id, const uint64_t index_id, QueryRangeInfo& range_info, PathHelper& helper);

  int check_has_exec_param(ObQueryRange& query_range, bool& has_exec_param);

  int get_preliminary_prefix_info(ObQueryRange& query_range, QueryRangeInfo& range_info);

  void get_prefix_info(
      ObKeyPart* key_part, int64_t& equal_prefix_count, int64_t& range_prefix_count, bool& contain_always_false);

  // @brief  check if an index is relevant to the conditions
  int is_relevant_index(const uint64_t table_id, const uint64_t index_ref_id, bool& relevant);

  int get_valid_index_ids(const uint64_t table_id, const uint64_t ref_table_id, ObIArray<uint64_t>& valid_index_id);
  // table heuristics
  int add_table_by_heuristics(const uint64_t table_id, const uint64_t ref_table_id,
      const ObIndexInfoCache& index_info_cache, const ObIArray<uint64_t>& valid_index_ids, bool& added,
      PathHelper& helper, ObIArray<AccessPath*>& access_paths);

  // table heuristics for a virtual table.
  int virtual_table_heuristics(const uint64_t table_id, const uint64_t ref_table_id,
      const ObIndexInfoCache& index_info_cache, const ObIArray<uint64_t>& valid_index_ids, uint64_t& index_to_use);

  // table heuristics for non-virtual table
  int user_table_heuristics(const uint64_t table_id, const uint64_t ref_table_id,
      const ObIndexInfoCache& index_info_cache, const ObIArray<uint64_t>& valid_index_ids, uint64_t& index_to_use,
      PathHelper& helper);

  int refine_table_heuristics_result(const uint64_t table_id, const uint64_t ref_table_id,
      const common::ObIArray<uint64_t>& candidate_refine_idx, const common::ObIArray<uint64_t>& match_unique_idx,
      const ObIndexInfoCache& index_info_cache, uint64_t& index_to_use);

  int check_index_subset(const OrderingInfo* first_ordering_info, const int64_t first_index_key_count,
      const OrderingInfo* second_ordering_info, const int64_t second_index_key_count, CompareRelation& status);

  int check_right_tree_use_join_method(const ObJoinOrder* right_tree, const common::ObIArray<ObRelIds>& use_idxs,
      bool& hinted, int8_t& need_path_types, int8_t hint_type);
  int check_right_tree_no_use_join_method(const ObJoinOrder* right_tree, const common::ObIArray<ObRelIds>& no_use_idxs,
      int8_t& no_use_type, int8_t hint_type);
  /**
   * @brief Get path types needed to genereate
   * @need_path_types the types needed to genereate.
   */
  int get_valid_path_types(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const ObJoinType join_type,
      const bool ignore_hint, int8_t& need_path_types);

  int make_mj_path(Path* left_path, Path* right_path, ObJoinType join_type,
      common::ObIArray<ObRawExpr*>& join_conditions, common::ObIArray<ObRawExpr*>& no_sort_conditions,
      common::ObIArray<ObRawExpr*>& join_filters, common::ObIArray<ObRawExpr*>& join_quals, Path** join_path);

  // find minimal cost path
  int find_minimal_cost_path(const common::ObIArray<Path*>& all_paths, Path*& minimal_cost_path);

  int deduce_const_exprs_and_ft_item_set();

  int compute_fd_item_set_for_table_scan(
      const uint64_t table_id, const uint64_t table_ref_id, const ObIArray<ObRawExpr*>& quals);

  int compute_fd_item_set_for_join(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree,
      const JoinInfo* join_info, const ObJoinType join_type);

  int compute_fd_item_set_for_inner_join(
      const ObJoinOrder* left_tree, const ObJoinOrder* right_tree, const JoinInfo* join_info);

  int compute_fd_item_set_for_semi_anti_join(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree,
      const JoinInfo* join_info, const ObJoinType join_type);

  int compute_fd_item_set_for_outer_join(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree,
      const JoinInfo* ljoin_info, const ObJoinType join_type);

  int compute_fd_item_set_for_subplan_scan(ObRawExprFactory& expr_factory, const EqualSets& equal_sets,
      const uint64_t table_id, ObDMLStmt& parent_stmt, const ObSelectStmt& child_stmt,
      const ObFdItemSet& input_fd_item_sets, ObFdItemSet& output_fd_item_sets);

  int compute_one_row_info_for_table_scan(ObIArray<AccessPath*>& access_paths);

  int compute_one_row_info_for_join(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree,
      const ObIArray<ObRawExpr*>& join_condition, const ObIArray<ObRawExpr*>& equal_join_condition,
      const ObJoinType join_type);

private:
  int find_matching_cond(const ObIArray<ObRawExpr*>& join_conditions, const OrderItem& left_ordering,
      const OrderItem& right_ordering, const EqualSets& equal_sets, int64_t& common_prefix_idx);

private:
  int compute_cost_and_prune_access_path(PathHelper& helper, ObIArray<AccessPath*>& access_paths);
  int revise_output_rows_after_creating_path(PathHelper& helper, ObIArray<AccessPath*>& access_paths);
  int fill_filters(const common::ObIArray<ObRawExpr*>& all_filters, const ObQueryRange* query_range,
      ObCostTableScanInfo& est_scan_cost_info, bool& is_nl_with_extended_range);

  int can_extract_unprecise_range(
      const uint64_t table_id, const ObRawExpr* filter, const ObBitSet<>& ex_prefix_column_bs, bool& can_extract);

  int get_estimate_task(ObIArray<ObRowCountEstTask>& task_list, const ObAddr& addr, ObRowCountEstTask*& task);
  int collect_table_est_info(const common::ObPartitionKey& pkey, obrpc::ObEstPartArg* est_arg);

  int collect_path_est_info(const AccessPath* path, const common::ObPartitionKey& pkey, obrpc::ObEstPartArg* est_arg);

  int estimate_rowcount_for_access_path(const uint64_t table_id, const uint64_t ref_table_id,
      const ObIArray<AccessPath*>& all_paths, const bool is_inner_path, const bool no_use_remote_est);

  inline bool can_use_remote_estimate(OptimizationMethod method)
  {
    return OptimizationMethod::RULE_BASED != method;
  }

  int process_storage_rowcount_estimation(ObIArray<ObRowCountEstTask>& tasks, const ObIArray<AccessPath*>& all_paths,
      obrpc::ObEstPartRes& all_path_results);

  int remote_storage_estimate_rowcount(const ObAddr& addr, const obrpc::ObEstPartArg& arg, obrpc::ObEstPartRes& result);

  int compute_table_rowcount_info(
      const obrpc::ObEstPartRes& result, const uint64_t table_id, const uint64_t ref_table_id);

  int construct_scan_range_batch(
      const ObIArray<ObNewRange>& scan_ranges, ObSimpleBatch& batch, const AccessPath* ap, const ObPartitionKey& pkey);

  int increase_diverse_path_count(AccessPath* ap);

  int get_first_n_scan_range(
      const ObIArray<ObNewRange>& scan_ranges, ObIArray<ObNewRange>& valid_ranges, int64_t range_count);

  int get_valid_scan_range(const ObIArray<ObNewRange>& scan_ranges, ObIArray<ObNewRange>& valid_ranges,
      const AccessPath* ap, const ObPartitionKey& pkey);

  int get_range_projector(const AccessPath* ap, const share::schema::ObPartitionLevel part_level,
      ObIArray<int64_t>& part_projector, ObIArray<int64_t>& sub_part_projector, ObIArray<int64_t>& gen_projector,
      ObIArray<int64_t>& sub_gen_projector);

  int get_scan_range_partitions(const ObNewRange& scan_range, const ObIArray<int64_t>& part_projector,
      const ObIArray<int64_t>& gen_projector, ObTableLocation& table_location, ObIArray<int64_t>& part_ids,
      const ObIArray<int64_t>* level_one_part_ids = NULL);

  int construct_partition_range(ObArenaAllocator& allocator, const ObNewRange& scan_range, ObNewRange& part_range,
      const ObIArray<int64_t>& part_projector);

  int get_partition_columns(ObDMLStmt& stmt, const int64_t table_id, const int64_t ref_table_id,
      const share::schema::ObPartitionLevel part_level, ObIArray<ColumnItem>& partition_columns,
      ObIArray<ColumnItem>& generate_columns);

  int extract_partition_columns(ObDMLStmt& stmt, const int64_t table_id, const ObRawExpr* part_expr,
      ObIArray<ColumnItem>& partition_columns, ObIArray<ColumnItem>& generate_columns,
      const bool is_generate_column = false);

  int add_partition_column(
      ObDMLStmt& stmt, const uint64_t table_id, const uint64_t column_id, ObIArray<ColumnItem>& partition_columns);

public:
  inline double get_average_output_row_size()
  {
    return avg_output_row_size_;
  }
  inline double get_average_output_row_size() const
  {
    return avg_output_row_size_;
  }
  inline void set_average_output_row_size(const double average_row_size)
  {
    avg_output_row_size_ = average_row_size;
  }

private:
  int choose_best_inner_path(const ObJoinOrder* left_tree, const ObJoinOrder* right_tree,
      const ObIArray<ObRawExpr*>& join_conditions, const ObJoinType join_type, const bool force_mat,
      ObIArray<Path*>& right_inner_paths);

  int check_match_remote_sharding(const Path* path, const ObAddr& server, bool& is_match) const;

  int check_and_remove_is_null_qual(const ObJoinType join_type, const ObRelIds& left_ids, const ObRelIds& right_ids,
      const ObIArray<ObRawExpr*>& quals, ObIArray<ObRawExpr*>& normal_quals, bool& left_has_is_null,
      bool& right_has_is_null);

  int calc_join_output_rows(
      const ObJoinOrder& left_tree, const ObJoinOrder& right_tree, const ObJoinType join_order, double& new_rows);

  int try_calculate_partitions_with_rowid(const uint64_t table_id, const uint64_t ref_table_id, const bool is_dml_table,
      ObIArray<AccessPath*>& access_paths, const common::ObIArray<ObRawExpr*>& cond_exprs,
      ObSqlSchemaGuard& schema_guard, const ParamStore& param_store, ObExecContext& exec_ctx,
      share::ObIPartitionLocationCache& loc_cache, ObSQLSessionInfo& session_info,
      ObTablePartitionInfo& table_partition_info, bool& calc_done);

  int find_all_rowid_param_index(
      const common::ObIArray<ObRawExpr*>& cond_exprs, common::ObIArray<int64_t>& param_indexes);

  int find_rowid_param_index_recursively(const ObRawExpr* raw_expr, common::ObIArray<int64_t>& param_indexes);

  int find_nl_with_param_path(const ObIArray<ObRawExpr*>& join_conditions, const ObRelIds& join_relids,
      ObJoinOrder& right_tree, ObIArray<Path*>& nl_with_param_path);

  int generate_inner_base_paths(const ObIArray<ObRawExpr*>& join_conditions, const ObRelIds& join_relids,
      ObJoinOrder& right_tree, InnerPathInfo& inner_path_info);

  int generate_inner_access_path(const ObIArray<ObRawExpr*>& join_conditions, const ObRelIds& join_relids,
      ObJoinOrder& right_tree, InnerPathInfo& inner_path_info);

  int check_inner_path_valid(const ObIArray<ObRawExpr*>& join_conditions, const ObRelIds& join_relids,
      ObJoinOrder& right_tree, ObIArray<ObRawExpr*>& pushdown_quals, ObIArray<ObRawExpr*>& param_pushdown_quals,
      ObIArray<std::pair<int64_t, ObRawExpr*>>& nl_params, bool& is_valid);

  int generate_inner_subquery_path(const ObIArray<ObRawExpr*>& join_conditions, const ObRelIds& join_relids,
      ObJoinOrder& right_tree, InnerPathInfo& inner_path_info);
  int generate_one_inner_subquery_path(ObDMLStmt& parent_stmt, const ObRelIds join_relids,
      const ObIArray<ObRawExpr*>& pushdown_quals, InnerPathInfo& inner_path_info);
  int check_and_fill_inner_path_info(PathHelper& helper, ObDMLStmt& stmt, InnerPathInfo& inner_path_info,
      const ObIArray<ObRawExpr*>& pushdown_quals, ObIArray<std::pair<int64_t, ObRawExpr*>>& nl_params);
  int extract_pushdown_quals(const ObIArray<ObRawExpr*>& quals, ObIArray<ObRawExpr*>& pushdown_quals);
  int get_range_params(const Path* path, ObIArray<ObRawExpr*>& range_exprs);
  InnerPathInfo* get_inner_path_info(const ObIArray<ObRawExpr*>& join_conditions);
  int check_contain_rownum(const ObIArray<ObRawExpr*>& join_condition, bool& contain_rownum);
  int extract_real_join_keys(ObIArray<ObRawExpr*>& join_keys);

  friend class ::test::TestJoinOrder_ob_join_order_param_check_Test;
  friend class ::test::TestJoinOrder_ob_join_order_src_Test;

private:
  common::ObIAllocator* allocator_;
  ObLogPlan* plan_;
  PathType type_;
  uint64_t table_id_;
  ObRelIds table_set_;
  ObRelIds output_table_set_;
  double output_rows_;
  double avg_output_row_size_;
  double anti_or_semi_match_sel_;              // for anti/semi join
  ObTablePartitionInfo table_partition_info_;  // destroy in ObJoinOrder
  ObShardingInfo sharding_info_;               // only for base table
  ObTableMetaInfo table_meta_info_;            // only for base table
  JoinInfo* join_info_;
  common::ObSEArray<ConflictDetector*, 8, common::ModulePageAllocator, true> used_conflict_detectors_;
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> restrict_info_set_;
  common::ObSEArray<Path*, 32, common::ModulePageAllocator, true> interesting_paths_;
  /*
   * we maintain equal set condition to generate equal sets bottom-up
   * ordering equal set conditions are used for eliminating sort
   */
  bool is_init_;
  EqualSets ordering_output_equal_sets_;

  // used to record const exprs from where condition
  bool is_at_most_one_row_;
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> const_exprs_;
  // this following variables are tracked to remember how base table access path are generated
  BaseTableOptInfo table_opt_info_;
  common::ObSEArray<AccessPath*, 4, common::ModulePageAllocator, true> available_access_paths_;
  int64_t diverse_path_count_;  // number of access path with diverse query range
  ObFdItemSet fd_item_set_;
  ObFdItemSet candi_fd_item_set_;
  ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> not_null_columns_;
  // cache for all inner path
  InnerPathInfos inner_path_infos_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObJoinOrder);
};
}  // namespace sql
}  // namespace oceanbase

#endif
