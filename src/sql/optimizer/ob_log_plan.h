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

#ifndef OCEANBASE_SQL_OB_LOG_PLAN_H
#define OCEANBASE_SQL_OB_LOG_PLAN_H
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/ddl/ob_explain_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_opt_est_utils.h"
#include "sql/optimizer/ob_opt_est_sel.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_table_partition_info.h"
#include "sql/optimizer/ob_optimizer.h"
#include "share/client_feedback/ob_feedback_int_struct.h"

namespace test {
class ObLogPlanTest_ob_explain_test_Test;
}

namespace oceanbase {

namespace share {
class ObServerLocality;
namespace schema {
class ObSchemaGetterGuard;
}
}  // namespace share

namespace common {
const double DEFAULT_SUBPLAN_FILTER_SEL = 1.0;
}

namespace storage {
class ObPartitionService;
}

namespace sql {
class ObLogicalOperator;
class ObLogTableScan;
class ObLogDelUpd;
class AllocExchContext;
class ObJoinOrder;
class AccessPath;
class Path;
class JoinPath;
class SubQueryPath;
class ObJoinOrder;
class ObOptimizerContext;
class ObLogJoin;
struct JoinInfo;
struct ConflictDetector;
class ObLogSort;
class ObLogSubPlanFilter;
class ObAllocExprContext;
class ObLogTempTableInsert;
class ObLogTempTableAccess;
class ObLogTempTableTransformation;

/**
 *  Explain plan text formatter
 */
struct plan_formatter {
  plan_formatter() : column_name(), num_of_columns(0), column_width()
  {}
  virtual ~plan_formatter()
  {}
  // constants
  const static int64_t max_plan_column_width = 500;
  const static int64_t max_plan_column = 20;

  // members
  const char* column_name[max_plan_column];
  int64_t num_of_columns;
  int32_t column_width[max_plan_column_width];
};

// define operatory enum type
#define KEYS_DEF                                                                                           \
  KEY_DEF(Id, "ID"), KEY_DEF(Operator, "OPERATOR"), KEY_DEF(Name, "NAME"), KEY_DEF(Est_Rows, "EST. ROWS"), \
      KEY_DEF(Cost, "COST"), KEY_DEF(Max_Plan_Column, "End")
#define KEY_DEF(identifier, name) identifier
enum ExplainColumnEnumType { KEYS_DEF };
#undef KEY_DEF

// each line of the explain plan text
struct plan_line {
  plan_line() : id(0), level(0), column_value()
  {}
  virtual ~plan_line()
  {}
  int id;
  int level;
  common::ObSEArray<common::ObString, 4, common::ModulePageAllocator, true> column_value;
};

// explain plan text
class planText {
public:
  static const int64_t SIMPLE_COLUMN_NUM = 3;
  planText(char* buffer, const int64_t buffer_len, ExplainType type)
      : level(0),
        buf(buffer),
        buf_len(buffer_len),
        pos(0),
        formatter(),
        format(type),
        is_inited_(false),
        is_oneline_(false),
        outline_type_(OUTLINE_TYPE_UNINIT)
  {}
  virtual ~planText()
  {}
  int init();

  int level;
  char* buf;
  int64_t buf_len;
  int64_t pos;
  plan_formatter formatter;
  ExplainType format;
  bool is_inited_;
  bool is_oneline_;
  OutlineType outline_type_;
};
#undef KYES_DEF

struct SubPlanInfo {
  SubPlanInfo() : init_expr_(NULL), subplan_(NULL), init_plan_(false), sp_params_()
  {}
  SubPlanInfo(ObRawExpr* expr, ObLogPlan* plan, bool init_)
      : init_expr_(expr), subplan_(plan), init_plan_(init_), sp_params_()
  {}
  virtual ~SubPlanInfo()
  {}
  int add_sp_params(common::ObIArray<std::pair<int64_t, ObRawExpr*>>& params)
  {
    return append(sp_params_, params);
  }
  void set_subplan(ObLogPlan* plan)
  {
    subplan_ = plan;
  }

  ObRawExpr* init_expr_;
  ObLogPlan* subplan_;
  bool init_plan_;
  common::ObSEArray<std::pair<int64_t, ObRawExpr*>, 16, common::ModulePageAllocator, true> sp_params_;
  TO_STRING_KV(K_(init_expr), K_(sp_params), K_(subplan), K_(init_plan));
};

typedef common::ObSEArray<ObJoinOrder*, 4> JoinOrderArray;

/**
 *  Base class for logical plan for all DML/select statements
 */
class ObLogPlan {
public:
  friend class ::test::ObLogPlanTest_ob_explain_test_Test;

  typedef common::ObList<common::ObAddr, common::ObArenaAllocator> ObAddrList;

  static int select_replicas(ObExecContext& exec_ctx, const common::ObIArray<const ObTableLocation*>& tbl_loc_list,
      const common::ObAddr& local_server, common::ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list);
  static int select_replicas(ObExecContext& exec_ctx, bool is_weak, const common::ObAddr& local_server,
      common::ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list);

public:
  ObLogPlan(ObOptimizerContext& ctx, const ObDMLStmt* stmt);
  virtual ~ObLogPlan();

  /* return the sql text for the plan */
  inline common::ObString& get_sql_text()
  {
    return sql_text_;
  }
  // @brief Get the corresponding stmt
  inline virtual const ObDMLStmt* get_stmt() const
  {
    return stmt_;
  }
  inline virtual ObDMLStmt* get_stmt()
  {
    return const_cast<ObDMLStmt*>(stmt_);
  }

  inline int get_stmt_type(stmt::StmtType& stmt_type) const
  {
    int ret = common::OB_NOT_INIT;
    if (NULL != get_stmt()) {
      stmt_type = get_stmt()->get_stmt_type();
      ret = common::OB_SUCCESS;
    }
    return ret;
  }

  double get_optimization_cost();

  void set_query_ref(ObQueryRefRawExpr* query_ref)
  {
    query_ref_ = query_ref;
  }
  // @brief Get the ptr to the root
  inline ObLogicalOperator* get_plan_root() const
  {
    return root_;
  }
  // @brief Set the root of the plan
  inline void set_plan_root(ObLogicalOperator* root)
  {
    root_ = root;
    if (NULL != query_ref_) {
      query_ref_->set_ref_operator(root);
    } else { /* do nothing */
    }
  }

  void set_max_op_id(uint64_t max_op_id)
  {
    max_op_id_ = max_op_id;
  }
  uint64_t get_max_op_id() const
  {
    return max_op_id_;
  }

  int get_current_best_plan(ObLogicalOperator*& best_plan);
  int set_final_plan_root(ObLogicalOperator* best_plan);
  int change_id_hint_to_idx();
  /**
   * @brief  Generate the plan tree
   * @param void
   * @retval OB_SUCCESS execute success
   * @retval OB_OPTIMIZE_GEN_PLAN_FALIED failed to generate the logical plan
   *
   * The function will be invoked by all DML/select statements and will handle
   * the 'common' part of those statements, including joins, order-by, limit and
   * etc.
   */
  virtual int generate_plan_tree();
  /**
   * Generate the "explain plan" string
   */
  int64_t to_string(char* buf, const int64_t buf_len, ExplainType type = EXPLAIN_TRADITIONAL) const;

  /**
   * Get optimizer context
   */
  ObOptimizerContext& get_optimizer_context() const
  {
    return optimizer_context_;
  }

  ObFdItemFactory& get_fd_item_factory() const
  {
    return optimizer_context_.get_fd_item_factory();
  }

  virtual int generate_raw_plan() = 0;
  /**
   *  GENERATE logical PLAN
   *
   *  The general public interface to generate a logical plan for a 'select' statement
   */
  virtual int generate_plan() = 0;
  /**
   *  Get allocator used in sql compilation
   *
   *  The entire optimization process should use only this allocator.
   */
  common::ObIAllocator& get_allocator() const
  {
    return allocator_;
  }
  /**
   *  Get the logical operator allocator
   */
  virtual ObLogOperatorFactory& get_log_op_factory()
  {
    return log_op_factory_;
  }
  virtual int plan_tree_copy(ObLogicalOperator* src, ObLogicalOperator*& dst);
  /**
   *  List all needed plan traversals
   */
  template <typename... TS>
  int plan_traverse_loop(TS... args);
  // Set plan type use AllocExchCtx1
  void set_phy_plan_type(AllocExchContext& ctx);
  ObPhyPlanType get_phy_plan_type() const
  {
    return phy_plan_type_;
  }
  void set_location_type(ObPhyPlanType location_type)
  {
    location_type_ = location_type;
  }
  ObPhyPlanType get_location_type() const
  {
    return location_type_;
  }
  const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*>>& get_group_replaced_exprs() const
  {
    return group_replaced_exprs_;
  }
  int set_group_replaced_exprs(common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*>>& exprs)
  {
    return group_replaced_exprs_.assign(exprs);
  }
  /**
   *  Set the plan type to a specific one
   */
  void set_phy_plan_type(ObPhyPlanType phy_plan_type)
  {
    phy_plan_type_ = phy_plan_type;
  }

  common::ObIArray<ObAcsIndexInfo*>& get_acs_index_info()
  {
    return acs_index_infos_;
  }
  const common::ObIArray<ObAcsIndexInfo*>& get_acs_index_info() const
  {
    return acs_index_infos_;
  }

  common::ObIArray<int64_t>& get_multi_stmt_rowkey_pos()
  {
    return multi_stmt_rowkey_pos_;
  }

  bool is_local_or_remote_plan() const
  {
    return OB_PHY_PLAN_REMOTE == phy_plan_type_ || OB_PHY_PLAN_LOCAL == phy_plan_type_;
  }

  const common::ObIArray<int64_t>& get_multi_stmt_rowkey_pos() const
  {
    return multi_stmt_rowkey_pos_;
  }
  int add_global_table_partition_info(ObTablePartitionInfo* addr_table_partition_info)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(get_stmt()) || OB_ISNULL(get_stmt()->get_query_ctx())) {
      ret = common::OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(get_stmt()->get_query_ctx()->table_partition_infos_.push_back(
                   addr_table_partition_info))) { /* Do nothing */
    } else {                                      /* Do nothing */
    }
    return ret;
  }

  int get_global_table_partition_info(common::ObIArray<ObTablePartitionInfo*>& info) const
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(get_stmt()) || OB_ISNULL(get_stmt()->get_query_ctx())) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      ret = append(info, get_stmt()->get_query_ctx()->table_partition_infos_);
    }
    return ret;
  }
  int get_global_phy_tbl_location_info(ObPhyTableLocationInfoIArray& tbl_infos) const;

  int remove_duplicate_constraint(ObLocationConstraintContext& location_constraint, ObSqlCtx& sql_ctx) const;
  int remove_duplicate_base_table_constraint(ObLocationConstraintContext& location_constraint) const;
  int remove_duplicate_pwj_constraint(ObIArray<ObPwjConstraint*>& pwj_constraints) const;
  int replace_pwj_constraints(ObIArray<ObPwjConstraint*>& constraints, const int64_t from, const int64_t to) const;
  int sort_pwj_constraint(ObLocationConstraintContext& location_constraint) const;

  inline int set_autoinc_params(common::ObIArray<share::AutoincParam>& autoinc_params)
  {
    return autoinc_params_.assign(autoinc_params);
  }
  inline common::ObIArray<share::AutoincParam>& get_autoinc_params()
  {
    return autoinc_params_;
  }
  inline int set_affected_last_insert_id(bool affected_last_insert_id)
  {
    affected_last_insert_id_ = affected_last_insert_id;
    return common::OB_SUCCESS;
  }
  inline bool get_affected_last_insert_id() const
  {
    return affected_last_insert_id_;
  }

  /** @brief Get order by columns */
  int get_order_by_exprs(ObLogicalOperator& root, common::ObIArray<ObRawExpr*>& order_by_exprs,
      common::ObIArray<ObOrderDirection>* directions = NULL);

  // @brief Make OrderItems by exprs
  int make_order_items(const common::ObIArray<ObRawExpr*>& exprs, const common::ObIArray<ObOrderDirection>* dirs,
      common::ObIArray<OrderItem>& items);

  int make_order_items(const common::ObIArray<ObRawExpr*>& exprs, common::ObIArray<OrderItem>& items);

  int make_order_items(const common::ObIArray<ObRawExpr*>& exprs, const common::ObIArray<ObOrderDirection>& dirs,
      common::ObIArray<OrderItem>& items);

  int get_table_items(const ObIArray<FromItem>& from_items, ObIArray<TableItem*>& table_items);

  int get_current_semi_infos(const ObIArray<SemiInfo*>& semi_infos, const ObIArray<TableItem*>& table_items,
      ObIArray<SemiInfo*>& current_semi_infos);

  int get_table_ids(TableItem* table_item, ObRelIds& table_ids);

  int get_table_ids(const ObIArray<uint64_t>& table_ids, ObRelIds& rel_ids);

  int get_table_ids(const ObIArray<TableItem*>& table_items, ObRelIds& table_ids);

  int get_table_ids(const ObIArray<ObRawExpr*>& exprs, ObRelIds& table_ids);

  int is_joined_table_filter(const ObIArray<TableItem*>& table_items, const ObRawExpr* expr, bool& is_filter);

  int find_inner_conflict_detector(const ObIArray<ConflictDetector*>& inner_conflict_detectors, const ObRelIds& rel_ids,
      ConflictDetector*& detector);

  int satisfy_associativity_rule(const ConflictDetector& left, const ConflictDetector& right, bool& is_satisfy);

  int satisfy_left_asscom_rule(const ConflictDetector& left, const ConflictDetector& right, bool& is_satisfy);

  int satisfy_right_asscom_rule(const ConflictDetector& left, const ConflictDetector& right, bool& is_satisfy);

  int add_conflict_rule(const ObRelIds& left, const ObRelIds& right, ObIArray<std::pair<ObRelIds, ObRelIds>>& rules);

  int generate_conflict_rule(ConflictDetector* left, ConflictDetector* right, bool is_left_child,
      ObIArray<std::pair<ObRelIds, ObRelIds>>& rules);

  inline ObIArray<ConflictDetector*>& get_conflict_detectors()
  {
    return conflict_detectors_;
  }
  inline const ObIArray<ConflictDetector*>& get_conflict_detectors() const
  {
    return conflict_detectors_;
  }

  int get_base_table_items(ObDMLStmt& stmt, const ObIArray<TableItem*>& table_items,
      const ObIArray<SemiInfo*>& semi_infos, ObIArray<TableItem*>& base_tables);

  int generate_base_level_join_order(
      const common::ObIArray<TableItem*>& table_items, common::ObIArray<ObJoinOrder*>& base_level);

  int select_location(ObIArray<ObTablePartitionInfo*>& tbl_part_info_list);

  int collect_partition_location_info(
      ObLogicalOperator* root, ObIArray<const ObPhyTableLocationInfo*>& phy_tbl_loc_info_list);

  int get_max_table_dop_for_plan(ObLogicalOperator* root, bool& disable_table_dop, int64_t& max_table_dop);

  int get_subplan(const ObRawExpr* expr, SubPlanInfo*& info);

  /**
   *  Get plan signature (hash value)
   */
  uint64_t get_signature() const
  {
    return hash_value_;
  }

  common::ObIArray<ObExprSelPair>& get_predicate_selectivities()
  {
    return predicate_selectivities_;
  }

  inline common::ObIArray<std::pair<int64_t, ObRawExpr*>>& get_onetime_exprs()
  {
    return onetime_exprs_;
  }

  int print_outline_oneline(char* buf, int64_t buf_len, int64_t& pos) const;

  // get expr selectivity from predicate_selectivities_
  double get_expr_selectivity(const ObRawExpr* expr, bool& found);

  // param_expr replace src_expr, need to store param_expr sel to predicate_selectivities.
  int store_param_expr_selectivity(ObRawExpr* src_expr, ObRawExpr* param_expr, ObEstSelInfo& sel_info);

  int can_be_late_materialization(bool& can_be);

  int check_late_materialization_project(const uint64_t table_id, const common::ObIArray<ObRawExpr*>& filter_exprs,
      const common::ObIArray<ObRawExpr*>& sort_exprs, const common::ObIArray<ObRawExpr*>& index_keys, bool& need,
      common::ObIArray<ObRawExpr*>& pre_access_columns, common::ObIArray<ObRawExpr*>& project_columns);

  int get_pre_project_cost(ObLogicalOperator* top, ObLogicalOperator* scan, common::ObIArray<ObRawExpr*>& index_columns,
      bool index_back, bool need_set, double& cost);

  int check_late_materialization_cost(ObLogicalOperator* top, ObLogicalOperator* scan,
      common::ObIArray<ObRawExpr*>& index_columns, common::ObIArray<ObRawExpr*>& table_columns, bool index_back,
      double min_cost, bool& need, double& get_cost, double& join_cost);

  int get_base_table_ids(common::ObIArray<uint64_t>& base_tables) const;
  static int select_one_server(
      const common::ObAddr& selected_server, common::ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list);

  void set_expected_worker_count(int64_t c)
  {
    expected_worker_count_ = c;
  }
  int64_t get_expected_worker_count() const
  {
    return expected_worker_count_;
  }
  void set_require_local_execution(bool v)
  {
    require_local_execution_ = v;
  }
  bool is_require_local_execution() const
  {
    return require_local_execution_;
  }
  int is_partition_in_same_server(const ObIArray<const ObPhyTableLocationInfo*>& phy_location_infos, bool& is_same,
      bool& multi_part_table, common::ObAddr& first_addr);

  int check_enable_plan_expiration(bool& enable) const;
  bool need_consistent_read() const;
  int check_fullfill_safe_update_mode(ObLogicalOperator* op);
  int do_check_fullfill_safe_update_mode(ObLogicalOperator* top, bool& is_not_fullfill);

public:
  struct CandidatePlan {
    CandidatePlan(ObLogicalOperator* plan_tree) : plan_tree_(plan_tree)
    {}
    CandidatePlan() : plan_tree_(NULL)
    {}
    virtual ~CandidatePlan()
    {}
    void reset()
    {
      plan_tree_ = NULL;
    }
    ObLogicalOperator* plan_tree_;

    int64_t to_string(char* buf, const int64_t buf_len) const
    {
      UNUSED(buf);
      UNUSED(buf_len);
      return common::OB_SUCCESS;
    }
  };

  struct All_Candidate_Plans {
    All_Candidate_Plans() : first_new_op_(NULL)
    {}
    virtual ~All_Candidate_Plans()
    {}
    common::ObSEArray<CandidatePlan, 8, common::ModulePageAllocator, true> candidate_plans_;
    std::pair<double, int64_t> plain_plan_;
    int get_best_plan(ObLogicalOperator*& best_plan)
    {
      int ret = common::OB_SUCCESS;
      best_plan = NULL;
      if (plain_plan_.second >= 0 && plain_plan_.second < candidate_plans_.count()) {
        best_plan = candidate_plans_.at(plain_plan_.second).plan_tree_;
      } else {
        ret = common::OB_ERR_UNEXPECTED;
      }
      return ret;
    }

    // the first new op allocated above the current candidate plans
    ObLogicalOperator* first_new_op_;
  };

  enum PlanRelationship {
    UNCOMPARABLE = -2,
    RIGHT_DOMINATED = -1,
    EQUAL = 0,
    LEFT_DOMINATED = 1,
  };

  /**
   * @brief Genearete a specified operator on top of a list of candidate plans
   * @param [out] jos - the generated Join_OrderS
   * @retval OB_SUCCESS execute success
   * @retval OB_SOME_ERROR special errno need to handle
   */
  int generate_join_orders();

  /** @brief Allcoate operator for access path */
  int allocate_access_path(AccessPath* ap, ObLogicalOperator*& out_access_path_op);

  int allocate_temp_table_path(AccessPath* ap, ObLogicalOperator*& out_access_path_op);

  int allocate_function_table_path(AccessPath* ap, ObLogicalOperator*& out_access_path_op);

  /*allocate table lookup operator*/
  int allocate_table_lookup(AccessPath& ap, common::ObString& table_name, common::ObString& index_name,
      ObLogicalOperator* child_node, common::ObIArray<ObRawExpr*>& filter_exprs, ObLogicalOperator*& output_op);

  int get_global_index_filters(const AccessPath& ap, common::ObIArray<ObRawExpr*>& global_index_filters,
      common::ObIArray<ObRawExpr*>& remaining_filters);

  // store index column ids including storing column.
  int store_index_column_ids(ObSqlSchemaGuard& schema_guard, ObLogTableScan& scan, const int64_t index_id);

  /** @brief Allcoate operator for join path */
  int allocate_join_path(JoinPath* join_path, ObLogicalOperator*& out_join_path_op);

  /** @brief Allcoate operator for subquery path */
  int allocate_subquery_path(SubQueryPath* subpath, ObLogicalOperator*& out_subquery_op);

  /** @brief Allcoate a ,aterial operator as parent of a path */
  int allocate_material_as_top(ObLogicalOperator*& old_top);

  /** @brief Create plan tree from an interesting order */
  int create_plan_tree_from_path(Path* path, ObLogicalOperator*& out_plan_tree);

  int extract_onetime_subquery_exprs(ObRawExpr* expr, ObIArray<ObRawExpr*>& onetime_subquery_exprs);

  /** @brief Initialize the candidate plans from join order */
  int candi_init();

  /** @brief Allocate ORDER BY on top of plan candidates */
  int candi_allocate_order_by(common::ObIArray<OrderItem>& order_items);
  int candi_allocate_subplan_filter_for_exprs(ObIArray<ObRawExpr*>& exprs);
  int allocate_sort_as_top(ObLogicalOperator*& old_top, const common::ObIArray<OrderItem>& sort_keys);
  int allocate_sort_as_top(ObLogicalOperator*& node, const common::ObIArray<ObRawExpr*>& order_columns,
      const common::ObIArray<ObOrderDirection>* directions);

  int allocate_group_by_as_top(ObLogicalOperator*& top, const AggregateAlgo algo,
      const ObIArray<ObRawExpr*>& group_by_exprs, const ObIArray<ObRawExpr*>& rollup_exprs,
      const ObIArray<ObAggFunRawExpr*>& agg_items, const ObIArray<ObRawExpr*>& having_exprs,
      const ObIArray<OrderItem>& expected_ordering, const bool from_pivot = false);

  /** @brief Allocate LIMIT on top of plan candidates */
  int candi_allocate_limit(common::ObIArray<OrderItem>& order_items);
  int candi_allocate_limit(ObRawExpr* limit_expr, ObRawExpr* offset_expr, ObRawExpr* percent_expr,
      common::ObIArray<OrderItem>& expect_ordering, const bool has_union_child, const bool is_calc_found_rows,
      const bool is_top_limit, const bool is_fetch_with_ties);

  int is_plan_reliable(const ObLogicalOperator* root, bool& is_reliable);

  /** @brief Allocate sequence op on top of plan candidates */
  int candi_allocate_sequence();
  int allocate_sequence_as_top(ObLogicalOperator*& old_top);

  /** @brief Allocate SELECTINTO on top of plan candidates */
  int candi_allocate_select_into();
  /** @brief allocate select into as new top(parent)**/
  int allocate_selectinto_as_top(ObLogicalOperator*& old_top);

  /// @param: top is the Top logical operator of plan.
  int if_plan_need_limit(ObLogicalOperator* top, ObRawExpr* limit_expr, ObRawExpr* offset_expr, ObRawExpr* percent_expr,
      bool is_fetch_with_ties, bool& need_alloc_limit);

  /** @brief allocate limit as new top(parent)**/
  int allocate_limit_as_top(ObLogicalOperator*& old_top, ObRawExpr* limit_expr, ObRawExpr* offset_expr,
      ObRawExpr* percent_expr, common::ObIArray<OrderItem>& expect_ordering, const bool has_union_child,
      const bool is_calc_found_rows, const bool is_top_limit, const bool is_fetch_with_ties);

  /**
   *  Plan tree traversing(both top-down and bottom-up)
   */
  int plan_tree_traverse(const TraverseOp& operation, void* ctx);

  inline void set_signature(uint64_t hash_value)
  {
    hash_value_ = hash_value;
  }

  int print_outline(planText& plan, bool is_hints = false) const;

  int candi_allocate_subplan_filter_for_where();
  int candi_allocate_subplan_filter(ObIArray<ObRawExpr*>& subquery_exprs, const bool is_filter);
  int candi_allocate_subplan_filter(
      ObIArray<ObRawExpr*>& subquery_exprs, const bool is_filter, All_Candidate_Plans& all_plans);
  int candi_allocate_filter(const ObIArray<ObRawExpr*>& filter_exprs);
  int generate_subplan_filter_info(const ObIArray<ObRawExpr*>& subquery_exprs, ObLogicalOperator* top_node,
      ObIArray<ObLogicalOperator*>& subquery_ops, ObIArray<std::pair<int64_t, ObRawExpr*>>& params,
      ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs, ObBitSet<>& initplan_idxs, ObBitSet<>& onetime_idxs);
  int allocate_subplan_filter_as_top(ObLogicalOperator*& top, const ObIArray<ObLogicalOperator*>& subquery_ops,
      const ObIArray<std::pair<int64_t, ObRawExpr*>>& params,
      const ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs, const ObBitSet<>& initplan_idxs,
      const ObBitSet<>& onetime_idxs, const ObIArray<ObRawExpr*>* filters);
  int allocate_subplan_filter_as_top(
      common::ObIArray<ObRawExpr*>& subquery_exprs, const bool is_filter, ObLogicalOperator*& old_top);
  int allocate_subplan_filter_as_below(common::ObIArray<ObRawExpr*>& exprs, ObLogicalOperator* top_node);
  int need_alloc_child_for_subplan_filter(ObLogicalOperator* root, ObLogPlan* sub_plan, bool& need);
  int adjust_stmt_onetime_exprs(ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs);
  int find_and_replace_onetime_expr_with_param(
      const ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs, ObIArray<ObRawExpr*>& ori_exprs);
  int find_and_replace_onetime_expr_with_param(
      const ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs, ObRawExpr*& ori_expr);

  int candi_allocate_count();
  int classify_rownum_exprs(const common::ObIArray<ObRawExpr*>& rownum_exprs,
      common::ObIArray<ObRawExpr*>& filter_exprs, common::ObIArray<ObRawExpr*>& start_exprs, ObRawExpr*& limit_expr);
  int classify_rownum_expr(const ObItemType expr_type, ObRawExpr* rownum_expr, ObRawExpr* const_expr,
      common::ObIArray<ObRawExpr*>& filter_exprs, common::ObIArray<ObRawExpr*>& start_exprs, ObRawExpr*& limit_expr);
  int allocate_count_as_top(ObLogicalOperator*& old_top, const common::ObIArray<ObRawExpr*>& filter_exprs,
      const common::ObIArray<ObRawExpr*>& start_exprs, ObRawExpr* limit_expr);

  All_Candidate_Plans& get_candidate_plans()
  {
    return candidates_;
  }

  const ObRawExprSets& get_empty_expr_sets()
  {
    return empty_expr_sets_;
  }
  const ObFdItemSet& get_empty_fd_item_set()
  {
    return empty_fd_item_set_;
  }
  const ObRelIds& get_empty_table_set()
  {
    return empty_table_set_;
  }
  inline common::ObIArray<ObRawExpr*>& get_subquery_filters()
  {
    return subquery_filters_;
  }
  int init_plan_info();
  EqualSets& get_equal_sets()
  {
    return equal_sets_;
  }
  const EqualSets& get_equal_sets() const
  {
    return equal_sets_;
  }
  int set_all_exprs(const ObAllocExprContext& ctx);
  const ObRawExprUniqueSet& get_all_exprs() const
  {
    return all_exprs_;
  };
  ObRawExprUniqueSet& get_all_exprs()
  {
    return all_exprs_;
  };

  EqualSets* create_equal_sets();
  ObEstSelInfo* create_est_sel_info(ObLogicalOperator* op);
  ObJoinOrder* create_join_order(PathType type);

  const common::ObIArray<SubPlanInfo*>& get_subplans() const
  {
    return subplan_infos_;
  }
  inline const ObEstSelInfo& get_est_sel_info() const
  {
    return est_sel_info_;
  }
  inline ObEstSelInfo& get_est_sel_info()
  {
    return est_sel_info_;
  }
  inline bool get_is_subplan_scan() const
  {
    return is_subplan_scan_;
  }
  inline void set_is_subplan_scan(bool is_subplan_scan)
  {
    is_subplan_scan_ = is_subplan_scan;
  }
  inline common::ObIArray<ObRawExpr*>& get_const_exprs()
  {
    return const_exprs_;
  }
  inline const common::ObIArray<ObRawExpr*>& get_const_exprs() const
  {
    return const_exprs_;
  }

  inline int add_pushdown_filters(const common::ObIArray<ObRawExpr*>& pushdown_filters)
  {
    return append(pushdown_filters_, pushdown_filters);
  }

  inline common::ObIArray<ObRawExpr*>& get_pushdown_filters()
  {
    return pushdown_filters_;
  }
  inline const common::ObIArray<ObRawExpr*>& get_pushdown_filters() const
  {
    return pushdown_filters_;
  }

  int get_subplan_info(const ObRawExpr* expr, common::ObIArray<SubPlanInfo*>& subplans);

  int extract_onetime_exprs(
      ObRawExpr* expr, common::ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs, common::ObBitSet<>& idxs);

  int generate_subplan(ObRawExpr*& expr);

  int add_startup_filters(common::ObIArray<ObRawExpr*>& exprs)
  {
    return append(startup_filters_, exprs);
  }

  inline common::ObIArray<ObRawExpr*>& get_startup_filters()
  {
    return startup_filters_;
  }

protected:
  int update_plans_interesting_order_info(ObIArray<CandidatePlan>& candidate_plans, const int64_t check_scope);

  int prune_and_keep_best_plans(ObIArray<CandidatePlan>& candidate_plans);

  int add_candidate_plan(common::ObIArray<CandidatePlan>& current_plans, const CandidatePlan& new_plan);

  int compute_plan_relationship(
      const CandidatePlan& first_plan, const CandidatePlan& second_plan, PlanRelationship& relation);

  int is_pipeline_operator(const CandidatePlan& first_plan, bool& is_pipeline);

  int distribute_onetime_subquery(ObRawExpr*& qual, bool& can_distribute);

  int distribute_subquery_qual(ObRawExpr*& qual, bool& can_distribute);

  int check_qual_can_distribute(ObRawExpr* qual, ObDMLStmt* stmt, bool& can_distribute);

  int extract_params_for_subplan_filter(ObRawExpr* expr, ObRawExpr*& new_expr);

  int extract_params_for_subplan_expr(ObRawExpr*& new_expr, ObRawExpr* expr);

  int try_split_or_qual(const ObRelIds& table_ids, ObOpRawExpr& or_qual, ObIArray<ObRawExpr*>& table_quals);

  int split_or_quals(const ObIArray<TableItem*>& table_items, ObIArray<ObRawExpr*>& quals);

  int split_or_quals(const ObIArray<TableItem*>& table_items, ObRawExpr* qual, ObIArray<ObRawExpr*>& new_quals);

  int pre_process_quals(
      const ObIArray<TableItem*>& table_items, const ObIArray<SemiInfo*>& semi_infos, ObIArray<ObRawExpr*>& quals);

  int pre_process_quals(SemiInfo* semi_info);

  int pre_process_quals(TableItem* table_item);

  int generate_conflict_detectors(const ObIArray<TableItem*>& table_items, const ObIArray<SemiInfo*>& semi_infos,
      ObIArray<ObRawExpr*>& quals, ObIArray<ObJoinOrder*>& baserels);

  int generate_semi_join_detectors(const ObIArray<SemiInfo*>& semi_infos, ObRelIds& left_rel_ids,
      const ObIArray<ConflictDetector*>& inner_join_detectors, ObIArray<ConflictDetector*>& semi_join_detectors);

  int generate_inner_join_detectors(const ObIArray<TableItem*>& table_items, ObIArray<ObRawExpr*>& quals,
      ObIArray<ObJoinOrder*>& baserels, ObIArray<ConflictDetector*>& inner_join_detectors);

  int generate_outer_join_detectors(TableItem* table_item, ObIArray<ObRawExpr*>& table_filter,
      ObIArray<ObJoinOrder*>& baserels, ObIArray<ConflictDetector*>& outer_join_detectors);

  int distribute_quals(
      TableItem* table_item, const ObIArray<ObRawExpr*>& table_filter, ObIArray<ObJoinOrder*>& baserels);

  int flatten_table_items(
      ObIArray<TableItem*>& table_items, ObIArray<ObRawExpr*>& table_filter, ObIArray<TableItem*>& flatten_table_items);

  int flatten_inner_join(TableItem* table_item, ObIArray<ObRawExpr*>& table_filter, ObIArray<TableItem*>& table_items);

  int inner_generate_outer_join_detectors(JoinedTable* joined_table, ObIArray<ObRawExpr*>& table_filter,
      ObIArray<ObJoinOrder*>& baserels, ObIArray<ConflictDetector*>& outer_join_detectors);

  int pushdown_where_filters(JoinedTable* joined_table, ObIArray<ObRawExpr*>& table_filter,
      ObIArray<ObRawExpr*>& left_quals, ObIArray<ObRawExpr*>& right_quals);

  int pushdown_on_conditions(
      JoinedTable* joined_table, ObIArray<ObRawExpr*>& left_quals, ObIArray<ObRawExpr*>& right_quals);

  int generate_cross_product_detector(const ObIArray<TableItem*>& table_items, ObIArray<ObRawExpr*>& quals,
      ObIArray<ConflictDetector*>& cross_product_detectors);

  int check_join_info(
      const ObRelIds& left, const ObRelIds& right, const ObIArray<ObRelIds>& base_table_ids, bool& is_connected);

  int generate_cross_product_conflict_rule(ConflictDetector* cross_product_detector,
      const ObIArray<TableItem*>& table_items, const ObIArray<ObRawExpr*>& join_conditions);

  int init_leading_info_from_joined_tables(TableItem* table);

  int init_leading_info_from_tables(const ObIArray<TableItem*>& table_items, const ObIArray<SemiInfo*>& semi_infos);

  int find_join_order_pair(
      uint8_t beg_pos, uint8_t& end_pos, uint8_t ignore_beg_pos, uint8_t& ignore_end_pos, bool& found);

  int get_table_ids_from_leading(uint8_t pos, ObRelIds& table_ids);

  int init_leading_info_from_leading_pair(uint8_t beg_pos, uint8_t end_pos, ObRelIds& table_set);

  int init_leading_info_from_leading();

  int init_leading_info(const ObIArray<TableItem*>& table_items, const ObIArray<SemiInfo*>& semi_infos);

  int init_bushy_tree_info(const ObIArray<TableItem*>& table_items);

  int init_bushy_tree_info_from_joined_tables(TableItem* table);

  int check_need_bushy_tree(common::ObIArray<JoinOrderArray>& join_rels, const int64_t join_level, bool& need);

  int generate_join_levels_with_DP(common::ObIArray<JoinOrderArray>& join_rels, const int64_t join_level);

  int generate_join_levels_with_linear(
      common::ObIArray<JoinOrderArray>& join_rels, const ObIArray<TableItem*>& table_items, const int64_t join_level);

  int inner_generate_join_levels_with_DP(
      common::ObIArray<JoinOrderArray>& join_rels, const int64_t join_level, bool ignore_hint);

  int inner_generate_join_levels_with_linear(common::ObIArray<JoinOrderArray>& join_rels, const int64_t join_level);

  int preprocess_for_linear(const ObIArray<TableItem*>& table_items, ObIArray<JoinOrderArray>& join_rels);

  int generate_join_order_with_table_tree(
      ObIArray<JoinOrderArray>& join_rels, TableItem* table, ObJoinOrder*& join_tree);

  int generate_single_join_level_with_DP(ObIArray<JoinOrderArray>& join_rels, uint32_t left_level, uint32_t right_level,
      uint32_t level, bool ignore_hint, bool& abort);

  int generate_single_join_level_with_linear(
      ObIArray<JoinOrderArray>& join_rels, uint32_t left_level, uint32_t right_level, uint32_t level);

  int inner_generate_join_order(ObIArray<JoinOrderArray>& join_rels, ObJoinOrder* left_tree, ObJoinOrder* right_tree,
      uint32_t level, bool force_order, bool delay_cross_product, bool& is_valid_join);

  int is_detector_used(ObJoinOrder* left_tree, ObJoinOrder* right_tree, ConflictDetector* detector, bool& is_used);

  int choose_join_info(ObJoinOrder* left_tree, ObJoinOrder* right_tree, ObIArray<ConflictDetector*>& valid_detectors,
      bool delay_cross_product, bool& is_strict_order);

  int check_join_info(const ObIArray<ConflictDetector*>& valid_detectors, ObJoinType& join_type, bool& is_valid);

  int merge_join_info(const ObIArray<ConflictDetector*>& valid_detectors, JoinInfo& join_info);

  int generate_subplan_for_query_ref(ObQueryRefRawExpr* query_ref);
  int get_query_ref_in_expr_tree(
      ObRawExpr* expr, int32_t expr_level, common::ObIArray<ObQueryRefRawExpr*>& query_refs) const;
  int get_query_ref_in_stmt(
      ObSelectStmt* child_stmt, int32_t expr_level, common::ObIArray<ObQueryRefRawExpr*>& query_refs) const;
  int extract_params_for_subplan_in_stmt(
      const int32_t level, common::ObIArray<std::pair<int64_t, ObRawExpr*>>& exec_params);

  int resolve_unsolvable_exprs(
      const int32_t level, ObDMLStmt* stmt, common::ObIArray<std::pair<int64_t, ObRawExpr*>>& params);
  int extract_stmt_params(
      ObDMLStmt* stmt, const int32_t level, common::ObIArray<std::pair<int64_t, ObRawExpr*>>& params);
  int extract_params(common::ObIArray<ObRawExpr*>& exprs, const int32_t level,
      common::ObIArray<std::pair<int64_t, ObRawExpr*>>& params);
  int extract_params(ObRawExpr*& exprs, const int32_t level, common::ObIArray<std::pair<int64_t, ObRawExpr*>>& params);
  int can_produce_by_upper_stmt(ObRawExpr* expr, const int32_t stmt_level, bool& can_produce);
  //@is_filter is condition expr
  int extract_param_for_generalized_column(ObRawExpr*& exprs, common::ObIArray<std::pair<int64_t, ObRawExpr*>>& params);
  int extract_join_table_condition_params(
      JoinedTable& join_table, int32_t level, common::ObIArray<std::pair<int64_t, ObRawExpr*>>& params);

  inline common::ObIArray<SubPlanInfo*>& get_subplans()
  {
    return subplan_infos_;
  }

  inline int64_t get_subplan_size()
  {
    return subplan_infos_.count();
  }
  int add_subplan(SubPlanInfo* plan)
  {
    return subplan_infos_.push_back(plan);
  }

  int get_subplan(int64_t idx, SubPlanInfo*& subplan_info)
  {
    int ret = common::OB_SUCCESS;
    subplan_info = NULL;
    if (idx < 0 || idx >= subplan_infos_.count()) {
      ret = common::OB_INDEX_OUT_OF_RANGE;
    } else {
      subplan_info = subplan_infos_.at(idx);
    }
    return ret;
  }

  int add_subquery_filter(ObRawExpr* qual)
  {
    return subquery_filters_.push_back(qual);
  }

  int extract_subquery_ids(ObRawExpr* expr, common::ObBitSet<>& idxs);

  int add_onetime_exprs(common::ObIArray<std::pair<int64_t, ObRawExpr*>>& exprs)
  {
    return append(onetime_exprs_, exprs);
  }

  bool is_correlated_expr(const ObStmt* stmt, const ObRawExpr* expr, const int32_t curlevel);

  int add_rownum_expr(ObRawExpr* expr)
  {
    return rownum_exprs_.push_back(expr);
  }

  inline common::ObIArray<ObRawExpr*>& get_rownum_exprs()
  {
    return rownum_exprs_;
  }

  int add_random_expr(ObRawExpr* expr)
  {
    return random_exprs_.push_back(expr);
  }

  inline common::ObIArray<ObRawExpr*>& get_random_exprs()
  {
    return random_exprs_;
  }

  int add_startup_filter(ObRawExpr* expr)
  {
    return startup_filters_.push_back(expr);
  }

  int add_user_var_filter(ObRawExpr* expr)
  {
    return user_var_filters_.push_back(expr);
  }

  inline common::ObIArray<ObRawExpr*>& get_user_var_filters()
  {
    return user_var_filters_;
  }

  int process_scalar_in(ObRawExpr*& expr);

  int find_base_rel(common::ObIArray<ObJoinOrder*>& base_level, int64_t table_idx, ObJoinOrder*& base_rel);

  int find_join_rel(common::ObIArray<ObJoinOrder*>& join_level, ObRelIds& relids, ObJoinOrder*& join_rel);

  // sort the rel array
  int sort_cur_rels(common::ObIArray<ObJoinOrder*>& cur_rels);

  int check_join_legal(const ObRelIds& left_set, const ObRelIds& right_set, ConflictDetector* detector,
      bool delay_cross_product, bool& legal);

  int check_join_hint(
      const ObRelIds& left_set, const ObRelIds& right_set, bool& match_hint, bool& is_legal, bool& is_strict_order);

  int extract_level_pseudo_as_param(
      const common::ObIArray<ObRawExpr*>& join_condition, common::ObIArray<std::pair<int64_t, ObRawExpr*>>& exec_param);
  int find_connect_by_level(ObRawExpr* qual, common::ObIArray<ObRawExpr*>& level_exprs);

  int calc_plan_resource();

private:  // member functions
  static int set_table_location(
      ObTaskExecutorCtx& task_exec_ctx, common::ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list);

  static int strong_select_replicas(const common::ObAddr& local_server,
      common::ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list, bool& is_hit_partition, bool sess_in_retry);
  static int weak_select_replicas(storage::ObPartitionService& partition_service, const common::ObAddr& local_server,
      ObRoutePolicyType route_type, bool proxy_priority_hit_support,
      common::ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list, bool& is_hit_partition,
      share::ObFollowerFirstFeedbackType& follower_first_feedback);
  static int calc_hit_partition_for_compat(const common::ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list,
      const common::ObAddr& local_server, bool& is_hit_partition, ObAddrList& intersect_servers);
  static int calc_follower_first_feedback(const common::ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list,
      const common::ObAddr& local_server, const ObAddrList& intersect_servers,
      share::ObFollowerFirstFeedbackType& follower_first_feedback);

  inline bool is_upper_stmt_column_ref(const ObRawExpr& qual, const ObDMLStmt& stmt) const;
  int add_connect_by_prior_exprs(ObLogJoin& log_join);
  static int calc_intersect_servers(const ObIArray<ObPhyTableLocationInfo*>& phy_tbl_loc_info_list,
      ObList<ObAddr, ObArenaAllocator>& candidate_server_list);
  int remove_null_direction_for_order_item(
      common::ObIArray<OrderItem>& order_items, common::ObIArray<ObOrderDirection>& directions);
  int try_split_or_qual(ObIArray<ObJoinOrder*>& baserels, ObOpRawExpr& or_qual);
  int calc_and_set_exec_pwj_map(ObLocationConstraintContext& location_constraint) const;

  int64_t check_pwj_cons(const ObPwjConstraint& pwj_cons,
      const common::ObIArray<LocationConstraint>& base_location_cons, ObIArray<PwjTable>& pwj_tables,
      ObPwjComparer& pwj_comparer, PWJPartitionIdMap& pwj_map) const;

private:
  static const int64_t DEFAULT_SEARCH_SPACE_RELS = 10;

protected:  // member variable
  ObOptimizerContext& optimizer_context_;
  common::ObIAllocator& allocator_;
  const ObDMLStmt* stmt_;
  ObLogOperatorFactory log_op_factory_;
  All_Candidate_Plans candidates_;
  ObPhyPlanType phy_plan_type_;
  ObPhyPlanType location_type_;
  common::ObSEArray<std::pair<ObRawExpr*, ObRawExpr*>, 4, common::ModulePageAllocator, true> group_replaced_exprs_;

  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> pushdown_filters_;

private:  // member variable
  ObQueryRefRawExpr* query_ref_;
  ObLogicalOperator* root_;    // root operator
  common::ObString sql_text_;  // SQL string
  uint64_t hash_value_;        // plan signature
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> subquery_filters_;
  common::ObSEArray<SubPlanInfo*, 4, common::ModulePageAllocator, true> subplan_infos_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> rownum_exprs_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> random_exprs_;
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> startup_filters_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> user_var_filters_;

private:
  struct LeadingInfo {
    TO_STRING_KV(K_(table_set), K_(left_table_set), K_(right_table_set));

    ObRelIds table_set_;
    ObRelIds left_table_set_;
    ObRelIds right_table_set_;
  };
  ObRelIds leading_tables_;
  common::ObSEArray<LeadingInfo, 8, common::ModulePageAllocator, true> leading_infos_;
  common::ObSEArray<ObRelIds, 8, common::ModulePageAllocator, true> bushy_tree_infos_;
  common::ObSEArray<std::pair<int64_t, ObRawExpr*>, 4, common::ModulePageAllocator, true> onetime_exprs_;
  common::ObSEArray<ObAcsIndexInfo*, 4, common::ModulePageAllocator, true> acs_index_infos_;
  common::ObSEArray<share::AutoincParam, 2, common::ModulePageAllocator, true> autoinc_params_;  // auto-increment param
  common::ObSEArray<ConflictDetector*, 8, common::ModulePageAllocator, true> conflict_detectors_;
  ObJoinOrder* join_order_;
  common::ObSEArray<ObExprSelPair, 16, common::ModulePageAllocator, true> predicate_selectivities_;
  bool affected_last_insert_id_;
  int64_t expected_worker_count_;
  common::ObSEArray<int64_t, 4, common::ModulePageAllocator, true> multi_stmt_rowkey_pos_;

  // used as default equal sets/ unique sets for ObLogicalOperator
  const ObRawExprSets empty_expr_sets_;
  const ObRelIds empty_table_set_;
  EqualSets equal_sets_;  // non strict equal sets for stmt_;

  ObRawExprUniqueSet all_exprs_;
  const ObFdItemSet empty_fd_item_set_;
  // save the maxinum of the logical operator id
  uint64_t max_op_id_;

  bool require_local_execution_;
  ObEstSelInfo est_sel_info_;
  bool is_subplan_scan_;

  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> const_exprs_;

  DISALLOW_COPY_AND_ASSIGN(ObLogPlan);
};

inline bool ObLogPlan::is_upper_stmt_column_ref(const ObRawExpr& qual, const ObDMLStmt& stmt) const
{
  return qual.is_column_ref_expr() && qual.get_expr_level() < stmt.get_current_level();
}

template <typename... TS>
int ObLogPlan::plan_traverse_loop(TS... args)
{
  int ret = common::OB_SUCCESS;
  TraverseOp ops[] = {args...};
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(ops); i++) {
    if (OB_FAIL(plan_tree_traverse(ops[i], NULL))) {
      SQL_OPT_LOG(WARN, "failed to do plan traverse", K(ret), "op", ops[i]);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_LOG_PLAN_H
