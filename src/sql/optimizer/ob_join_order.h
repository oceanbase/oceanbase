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
#include "lib/container/ob_bit_set.h"
#include "sql/optimizer/ob_fd_item.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_plan.h"

using oceanbase::common::ObString;
namespace test
{
class TestJoinOrder_ob_join_order_param_check_Test;
class TestJoinOrder_ob_join_order_src_Test;
}

namespace oceanbase
{
namespace obrpc
{
class ObSrvRpcProxy;
}
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace sql
{
  class ObJoinOrder;
  class ObIndexSkylineDim;
  class ObIndexInfoCache;
  class ObSelectLogPlan;
  class ObConflictDetector;
  struct CandiRangeExprs
  {
    int64_t column_id_;
    int64_t index_;
    ObSEArray<ObRawExpr*, 2, common::ModulePageAllocator, true> eq_exprs_;
    ObSEArray<ObRawExpr*, 2, common::ModulePageAllocator, true> in_exprs_;
    TO_STRING_KV(
      K_(column_id),
      K_(index),
      K_(eq_exprs),
      K_(in_exprs)
    );
  };
  /*
   * 用于指示inner join未来的连接条件
   */
  struct JoinInfo;

  struct ValidPathInfo
  {
    ValidPathInfo() :
        join_type_(UNKNOWN_JOIN),
        local_methods_(0),
        distributed_methods_(0),
        force_slave_mapping_(false),
        force_mat_(false),
        force_no_mat_(false),
        prune_mj_(true),
        force_inner_nl_(false),
        ignore_hint_(true),
        is_reverse_path_(false) { }
    virtual ~ValidPathInfo() {};
    void reset()
    {
      join_type_ = UNKNOWN_JOIN;
      local_methods_ = 0;
      distributed_methods_ = 0;
      force_slave_mapping_ = false;
      force_mat_ = false;
      force_no_mat_ = false;
      prune_mj_ = true;
      force_inner_nl_ = false;
      ignore_hint_ = true;
      is_reverse_path_ = false;
    }
    TO_STRING_KV(K_(join_type),
                 K_(local_methods),
                 K_(distributed_methods),
                 K_(force_slave_mapping),
                 K_(force_mat),
                 K_(force_no_mat),
                 K_(prune_mj),
                 K_(force_inner_nl),
                 K_(ignore_hint),
                 K_(is_reverse_path));
    ObJoinType join_type_;
    int64_t local_methods_;
    int64_t distributed_methods_;
    bool force_slave_mapping_;  // force to use slave mapping
    bool force_mat_;    // force to add material
    bool force_no_mat_; // force to not add material
    bool prune_mj_; // prune merge join path
    bool force_inner_nl_;
    bool ignore_hint_;
    bool is_reverse_path_;
  };

  enum OptimizationMethod
  {
    RULE_BASED = 0,
    COST_BASED,
    MAX_METHOD
  };

  enum HeuristicRule
  {
    UNIQUE_INDEX_WITHOUT_INDEXBACK = 0,
    UNIQUE_INDEX_WITH_INDEXBACK,
    VIRTUAL_TABLE_HEURISTIC, // only for virtual table
    MAX_RULE
  };

  struct BaseTableOptInfo {
    BaseTableOptInfo ()
    : optimization_method_(OptimizationMethod::MAX_METHOD),
      heuristic_rule_(HeuristicRule::MAX_RULE),
      available_index_id_(),
      available_index_name_(),
      pruned_index_name_(),
      unstable_index_name_()
    {}

    // this following variables are tracked to remember how base table access path are generated
    OptimizationMethod optimization_method_;
    HeuristicRule heuristic_rule_;
    common::ObSEArray<uint64_t, 4, common::ModulePageAllocator, true> available_index_id_;
    common::ObSEArray<common::ObString, 4, common::ModulePageAllocator, true> available_index_name_;
    common::ObSEArray<common::ObString, 4, common::ModulePageAllocator, true> pruned_index_name_;
    common::ObSEArray<common::ObString, 4, common::ModulePageAllocator, true> unstable_index_name_;
  };
  struct JoinFilterInfo {
  JoinFilterInfo()
  : lexprs_(),
    rexprs_(),
    sharding_(NULL),
    calc_part_id_expr_(NULL),
    ref_table_id_(OB_INVALID_ID),
    index_id_(OB_INVALID_ID),
    table_id_(OB_INVALID_ID),
    filter_table_id_(OB_INVALID_ID),
    row_count_(1.0),
    join_filter_selectivity_(1.0),
    right_distinct_card_(1.0),
    need_partition_join_filter_(false),
    can_use_join_filter_(false),
    force_filter_(NULL),
    force_part_filter_(NULL),
    pushdown_filter_table_(),
    in_current_dfo_(true),
    skip_subpart_(false),
    use_column_store_(false) {}

  TO_STRING_KV(
    K_(lexprs),
    K_(rexprs),
    K_(sharding),
    K_(calc_part_id_expr),
    K_(ref_table_id),
    K_(index_id),
    K_(table_id),
    K_(filter_table_id),
    K_(row_count),
    K_(join_filter_selectivity),
    K_(need_partition_join_filter),
    K_(can_use_join_filter),
    K_(force_filter),
    K_(force_part_filter),
    K_(in_current_dfo),
    K_(skip_subpart),
    K_(use_column_store)
  );

  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> lexprs_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> rexprs_;
  ObShardingInfo *sharding_;      //join filter use基表的sharding
  ObRawExpr *calc_part_id_expr_;  //partition join filter计算分区id的表达式
  uint64_t ref_table_id_;         //join filter use基表的ref table id
  uint64_t index_id_;             //index id for join filter use
  uint64_t table_id_;             //join filter use基表的table id
  uint64_t filter_table_id_;        //join filter use实际受hint控制的table id
  double row_count_;              //join filter use基表的output rows
  double join_filter_selectivity_;
  double right_distinct_card_;
  bool need_partition_join_filter_;
  bool can_use_join_filter_;
  const ObJoinFilterHint *force_filter_;
  const ObJoinFilterHint *force_part_filter_;
  ObTableInHint pushdown_filter_table_;
  bool in_current_dfo_;
  // Indicates that part bf is only generated for the 1-level partition in the 2-level partition
  // If the table is a 1-level partition, this value is false.
  bool skip_subpart_;
  bool use_column_store_;
};

struct EstimateCostInfo {
  EstimateCostInfo()
    :join_filter_infos_(),
    need_row_count_(-1),  //no need to refine row count
    need_parallel_(ObGlobalHint::UNSET_PARALLEL),  //no need to refine parallel
    override_(false) {}

  void reset() {
    join_filter_infos_.reuse();
    need_row_count_ = -1;
    need_parallel_ = ObGlobalHint::UNSET_PARALLEL;
    override_ = false;
  }
  int assign(const EstimateCostInfo& other) {
    need_row_count_ = other.need_row_count_;
    need_parallel_ = other.need_parallel_;
    override_ = other.override_;
    return join_filter_infos_.assign(other.join_filter_infos_);
  }
  bool need_re_est(int64_t cur_parallel, double cur_row_count) const {
    return override_ || !join_filter_infos_.empty()
        || (ObGlobalHint::UNSET_PARALLEL != need_parallel_ && need_parallel_ != cur_parallel)
        || (need_row_count_ >= 0 && need_row_count_ < cur_row_count);
  }

  TO_STRING_KV(
    K_(join_filter_infos),
    K_(need_row_count),
    K_(need_parallel),
    K_(override)
  );

  ObSEArray<JoinFilterInfo, 4> join_filter_infos_;
  double need_row_count_;
  int64_t need_parallel_;
  bool override_;
};

  class Path
  {
  public:
    Path()
    : parent_(NULL),
      is_local_order_(false),
      is_range_order_(false),
      ordering_(),
      interesting_order_info_(OrderingFlag::NOT_MATCH),
      filter_(),
      cost_(0.0),
      op_cost_(0.0),
      log_op_(NULL),
      is_inner_path_(false),
      inner_row_count_(0),
      pushdown_filters_(),
      nl_params_(),
      strong_sharding_(NULL),
      weak_sharding_(),
      exchange_allocated_(false),
      phy_plan_type_(ObPhyPlanType::OB_PHY_PLAN_UNINITIALIZED),
      location_type_(ObPhyPlanType::OB_PHY_PLAN_UNINITIALIZED),
      contain_fake_cte_(false),
      contain_pw_merge_op_(false),
      contain_match_all_fake_cte_(false),
      contain_das_op_(false),
      parallel_(ObGlobalHint::UNSET_PARALLEL),
      op_parallel_rule_(OpParallelRule::OP_DOP_RULE_MAX),
      available_parallel_(ObGlobalHint::DEFAULT_PARALLEL),
      server_cnt_(1)
    {  }
    Path(ObJoinOrder* parent)
      : parent_(parent),
        is_local_order_(false),
        is_range_order_(false),
        ordering_(),
        interesting_order_info_(OrderingFlag::NOT_MATCH),
        filter_(),
        cost_(0.0),
        op_cost_(0.0),
        log_op_(NULL),
        is_inner_path_(false),
        inner_row_count_(0),
        pushdown_filters_(),
        nl_params_(),
        strong_sharding_(NULL),
        weak_sharding_(),
        exchange_allocated_(false),
        phy_plan_type_(ObPhyPlanType::OB_PHY_PLAN_UNINITIALIZED),
        location_type_(ObPhyPlanType::OB_PHY_PLAN_UNINITIALIZED),
        contain_fake_cte_(false),
        contain_pw_merge_op_(false),
        contain_match_all_fake_cte_(false),
        contain_das_op_(false),
        parallel_(ObGlobalHint::UNSET_PARALLEL),
        op_parallel_rule_(OpParallelRule::OP_DOP_RULE_MAX),
        available_parallel_(ObGlobalHint::DEFAULT_PARALLEL),
        server_cnt_(1),
        server_list_(),
        is_pipelined_path_(false),
        is_nl_style_pipelined_path_(false)
    {  }
    virtual ~Path() {}
    int assign(const Path &other, common::ObIAllocator *allocator);
    bool is_cte_path() const;
    bool is_function_table_path() const;
    bool is_json_table_path() const;
    bool is_temp_table_path() const;
    bool is_access_path() const;
    bool is_join_path() const;
    bool is_subquery_path() const;
    bool is_values_table_path() const;
    int check_is_base_table(bool &is_base_table);
    inline const common::ObIArray<OrderItem> &get_ordering() const { return ordering_; }
    inline common::ObIArray<OrderItem> &get_ordering() { return ordering_; }
    inline const common::ObIArray<ObAddr> &get_server_list() const { return server_list_; }
    inline common::ObIArray<ObAddr> &get_server_list() { return server_list_; }
    inline int64_t get_interesting_order_info() const { return interesting_order_info_; }
    inline void set_interesting_order_info(int64_t info) { interesting_order_info_ = info; }
    inline void add_interesting_order_flag(OrderingFlag flag) { interesting_order_info_ |= flag; }
    inline void add_interesting_order_flag(int64_t flags) { interesting_order_info_ |= flags; }
    inline void clear_interesting_order_flag(OrderingFlag flag){ interesting_order_info_ &= ~flag; }
    inline bool has_interesting_order_flag(OrderingFlag flag) const
    { return (interesting_order_info_ & flag) > 0; }
    inline bool has_interesting_order() const { return interesting_order_info_ > 0; }
    bool is_inner_path() const { return is_inner_path_; }
    void set_is_inner_path(bool is) { is_inner_path_ = is; }
    double get_cost() const { return cost_; }
    inline const ObShardingInfo *get_strong_sharding() const { return strong_sharding_; };
    inline ObShardingInfo *get_strong_sharding() { return strong_sharding_; }
    inline const ObIArray<ObShardingInfo*> &get_weak_sharding() const { return weak_sharding_; }
    inline ObIArray<ObShardingInfo*> &get_weak_sharding() { return weak_sharding_; }
    inline ObShardingInfo* get_sharding() const
    {
      ObShardingInfo *ret_sharding = NULL;
      if (NULL != strong_sharding_) {
        ret_sharding = strong_sharding_;
      } else if (!weak_sharding_.empty()) {
        ret_sharding = weak_sharding_.at(0);
      }
      return ret_sharding;
    }
    inline ObShardingInfo* try_get_sharding_with_table_location() const
    {
      ObShardingInfo *ret_sharding = NULL;
      if (NULL != strong_sharding_ && NULL != strong_sharding_->get_phy_table_location_info()) {
        ret_sharding = strong_sharding_;
      } else if (!weak_sharding_.empty() && NULL != weak_sharding_.at(0)
                 && NULL != weak_sharding_.at(0)->get_phy_table_location_info()) {
        ret_sharding = weak_sharding_.at(0);
      }
      return ret_sharding;
    }
    inline bool is_local() const
    {
      return (NULL != strong_sharding_ && strong_sharding_->is_local());
    }
    bool is_valid() const;
    inline bool is_remote() const
    {
      return (NULL != strong_sharding_ && strong_sharding_->is_remote());
    }
    inline bool is_match_all() const
    {
      return (NULL != strong_sharding_ && strong_sharding_->is_match_all());
    }
    inline bool is_distributed() const
    {
      return (NULL != strong_sharding_ && strong_sharding_->is_distributed())
              || !weak_sharding_.empty();
    }
    inline bool is_sharding() const
    {
      return is_remote() || is_distributed();
    }
    inline bool is_single() const
    {
      return is_local() || is_remote() || is_match_all();
    }
    virtual int estimate_cost()=0;
    virtual int re_estimate_cost(EstimateCostInfo &info, double &card, double &cost);
    double get_path_output_rows() const;
    bool contain_fake_cte() const { return contain_fake_cte_; }
    bool contain_pw_merge_op() const { return contain_pw_merge_op_; }
    bool contain_match_all_fake_cte() const { return contain_match_all_fake_cte_; }
    bool is_pipelined_path() const { return is_pipelined_path_; }
    bool is_nl_style_pipelined_path() const { return is_nl_style_pipelined_path_; }
    virtual int compute_pipeline_info();
    bool contain_das_op() const { return contain_das_op_; }
    virtual int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos) const = 0;
    int get_name(char *buf, const int64_t buf_len, int64_t &pos)
    {
      int ret = common::OB_SUCCESS;
      pos = 0;
      get_name_internal(buf, buf_len, pos);
      common::ObIArray<OrderItem> &ordering = ordering_;
      if (OB_FAIL(BUF_PRINTF("("))) { /* Do nothing */
      }
      if (OB_FAIL(ret)) { /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF(", "))) { /* Do nothing */
      } else if (OB_FAIL(BUF_PRINTF("cost=%f", cost_))) { /* Do nothing */
      } else {
        ret = BUF_PRINTF(")");
      }
      return ret;
    }
    inline bool parallel_more_than_part_cnt() const
    {
      const ObShardingInfo *sharding = try_get_sharding_with_table_location();
      return NULL != sharding && parallel_ > sharding->get_part_cnt();
    }
    int compute_path_property_from_log_op();
    int set_parallel_and_server_info_for_match_all();
    TO_STRING_KV(K_(is_local_order),
                 K_(ordering),
                 K_(interesting_order_info),
                 K_(cost),
                 K_(op_cost),
                 K_(is_inner_path),
                 K_(inner_row_count),
                 K_(filter),
                 K_(pushdown_filters),
                 K_(nl_params),
                 K_(exchange_allocated),
                 K_(phy_plan_type),
                 K_(location_type),
                 K_(is_pipelined_path),
                 K_(is_nl_style_pipelined_path));
  public:
    /**
     * 表示当前join order最终的父join order节点
     * 为了在生成下层join order时能够窥探后续outer join, semi join的连接条件, 需要递归地设置parent_
     */
    ObJoinOrder* parent_;
    bool is_local_order_;
    bool is_range_order_;
    common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> ordering_;//Path的输出序，不一定来自于Stmt上的expr
    int64_t interesting_order_info_;  // 记录path的序在stmt中的哪些地方用到 e.g. join, group by, order by
    common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> filter_;//基类的过滤条件：对于scan和subquery是scan_filter_，对于join是join_qual_
    double cost_;
    double op_cost_;
    ObLogicalOperator *log_op_;
    bool is_inner_path_; // inner path with push down filters
    double inner_row_count_; // inner path output row count
    common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> pushdown_filters_; // original push down filters(without being replaced by ?) for inner path
    common::ObSEArray<ObExecParamRawExpr *, 4, common::ModulePageAllocator, true> nl_params_; // parameters for inner path
    common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> subquery_exprs_;
    ObShardingInfo *strong_sharding_; // may be null
    // weak sharding is used for partition-wise-join check, thus it should have table location, otherwise it is meaningless
    common::ObSEArray<ObShardingInfo*, 8, common::ModulePageAllocator, true> weak_sharding_;
    common::ObSEArray<ObPCParamEqualInfo, 4, common::ModulePageAllocator, true> equal_param_constraints_;
    common::ObSEArray<ObPCConstParamInfo, 4, common::ModulePageAllocator, true> const_param_constraints_;
    common::ObSEArray<ObExprConstraint, 4, common::ModulePageAllocator, true> expr_constraints_;
    bool exchange_allocated_;
    ObPhyPlanType phy_plan_type_;
    ObPhyPlanType location_type_;
    bool contain_fake_cte_;
    bool contain_pw_merge_op_;
    bool contain_match_all_fake_cte_;
    bool contain_das_op_;
    // remember the parallel info to get this sharding
    int64_t parallel_;
    OpParallelRule op_parallel_rule_;
    int64_t available_parallel_; // parallel degree used by serial path to enable parallel again
    int64_t server_cnt_;
    common::ObSEArray<common::ObAddr, 8, common::ModulePageAllocator, true> server_list_;
    bool is_pipelined_path_;
    bool is_nl_style_pipelined_path_;

  private:
    DISALLOW_COPY_AND_ASSIGN(Path);
  };


  enum OptSkipScanState
  {
    SS_DISABLE = 0,
    SS_UNSET,
    SS_HINT_ENABLE,
    SS_NDV_SEL_ENABLE
  };
  class AccessPath : public Path
  {
  public:
    AccessPath(uint64_t table_id,
               uint64_t ref_table_id,
               uint64_t index_id,
               ObJoinOrder* parent,
               ObOrderDirection direction)
      : Path(parent),
        table_id_(table_id),
        ref_table_id_(ref_table_id),
        index_id_(index_id),
        is_global_index_(false),
        use_das_(false),
        table_partition_info_(NULL),
        index_keys_(),
        pre_query_range_(NULL),
        is_get_(false),
        order_direction_(direction),
        is_hash_index_(false),
        est_cost_info_(table_id,
                       ref_table_id,
                       index_id),
        est_records_(),
        range_prefix_count_(0),
        table_opt_info_(),
        for_update_(false),
        use_skip_scan_(OptSkipScanState::SS_UNSET),
        use_column_store_(false),
        is_valid_inner_path_(false)
    {
    }
    virtual ~AccessPath() {
    }
    int assign(const AccessPath &other, common::ObIAllocator *allocator);
    uint64_t get_table_id() const { return table_id_; }
    void set_table_id(uint64_t table_id) { table_id_ = table_id; }
    uint64_t get_ref_table_id() const { return ref_table_id_; }
    void set_ref_table_id(uint64_t ref_id) { ref_table_id_ = ref_id; }
    uint64_t get_index_table_id() const { return index_id_; }
    void set_index_table_id(uint64_t index_id) { index_id_ = index_id; }
    uint64_t get_repartition_ref_table_id() const {
      return is_global_index_ ? index_id_ : ref_table_id_;
    }
    bool is_get() const { return is_get_; }
    void set_is_get(bool is_get) { is_get_ = is_get; }
    double get_table_row_count() const
    { return est_cost_info_.table_meta_info_ == NULL ? 1.0 : est_cost_info_.table_meta_info_->table_row_count_; }
    double get_output_row_count() const { return est_cost_info_.output_row_count_; }
    double get_logical_query_range_row_count() const { return est_cost_info_.logical_query_range_row_count_; }
    double get_phy_query_range_row_count() const { return est_cost_info_.phy_query_range_row_count_; }
    double get_index_back_row_count() const { return est_cost_info_.index_back_row_count_; }
    double get_cost() { return cost_; }
    const ObCostTableScanInfo &get_cost_table_scan_info() const
    { return est_cost_info_; }
    ObCostTableScanInfo &get_cost_table_scan_info() { return est_cost_info_; }
    int compute_parallel_degree(const int64_t cur_min_parallel_degree,
                                int64_t &parallel);
    int check_and_prepare_estimate_parallel_params(const int64_t cur_min_parallel_degree,
                                                   int64_t &px_part_gi_min_part_per_dop,
                                                   double &cost_threshold_us,
                                                   int64_t &server_cnt,
                                                   int64_t &cur_parallel_degree_limit) const;
    int prepare_estimate_parallel(const int64_t pre_parallel,
                                  const int64_t parallel_degree_limit,
                                  const double cost_threshold_us,
                                  const int64_t server_cnt,
                                  const int64_t px_part_gi_min_part_per_dop,
                                  const double px_cost,
                                  const double cost,
                                  int64_t &cur_parallel,
                                  double &part_cnt_per_dop) const;
    int estimate_cost_for_parallel(const int64_t cur_parallel,
                                   const double part_cnt_per_dop,
                                   double &px_cost,
                                   double &cost);
    virtual int estimate_cost() override;
    virtual int re_estimate_cost(EstimateCostInfo &info, double &card, double &cost) override;
    static int re_estimate_cost(const EstimateCostInfo &param,
                                ObCostTableScanInfo &est_cost_info,
                                const SampleInfo &sample_info,
                                const ObOptimizerContext &opt_ctx,
                                double &card,
                                double &cost);
    int check_adj_index_cost_valid(double &stats_phy_query_range_row_count,
                                   double &stats_logical_query_range_row_count,
                                   int64_t &opt_stats_cost_percent,
                                   bool &is_valid) const;
    inline bool can_use_remote_estimate()
    {
      return NULL == table_opt_info_ ? false :
            OptimizationMethod::RULE_BASED != table_opt_info_->optimization_method_;
    }
    const ObIArray<ObNewRange> &get_query_ranges() const;
    virtual int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos) const
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(BUF_PRINTF("@"))) {
      } else if (OB_FAIL(BUF_PRINTF("%lu", table_id_))) {
      }
      return ret;
    }
    // compute current path is inner path and contribute query ranges
    int compute_valid_inner_path();
    inline bool is_false_range()
    {
      return 1 == est_cost_info_.ranges_.count() && est_cost_info_.ranges_.at(0).is_false_range();
    }

    TO_STRING_KV(K_(table_id),
                 K_(ref_table_id),
                 K_(index_id),
                 K_(op_cost),
                 K_(cost),
                 K_(ordering),
                 K_(is_local_order),
                 K_(is_get),
                 K_(order_direction),
                 K_(is_hash_index),
                 K_(est_cost_info),
                 K_(sample_info),
                 K_(range_prefix_count),
                 K_(for_update),
                 K_(use_das),
                 K_(use_skip_scan),
                 K_(use_column_store),
                 K_(is_valid_inner_path));
  public:
    //member variables
    uint64_t table_id_;
    uint64_t ref_table_id_;
    uint64_t index_id_;
    bool is_global_index_;
    bool use_das_;
    ObTablePartitionInfo *table_partition_info_;
    common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> index_keys_; // index keys
    ObQueryRange* pre_query_range_; // pre_query_range for each access path
    bool is_get_;
    ObOrderDirection order_direction_;//序的方向（升序or倒序）
    bool is_hash_index_;  // is hash index (virtual table and is index)
    ObCostTableScanInfo est_cost_info_; // estimate cost info
    common::ObSEArray<ObEstRowCountRecord, 2,
                      common::ModulePageAllocator, true> est_records_;
    SampleInfo sample_info_; // sample scan info
    int64_t range_prefix_count_; // prefix count
    BaseTableOptInfo *table_opt_info_;
    bool for_update_;
    OptSkipScanState use_skip_scan_;
    bool use_column_store_;
    // mark this access path is inner path and contribute query range
    bool is_valid_inner_path_;
  private:
    DISALLOW_COPY_AND_ASSIGN(AccessPath);
  };

  class JoinPath : public Path
  {
  public:
  JoinPath()
    : Path(NULL),
      left_path_(NULL),
      right_path_(NULL),
      join_algo_(INVALID_JOIN_ALGO),
      join_dist_algo_(DistAlgo::DIST_INVALID_METHOD),
      is_slave_mapping_(false),
      use_hybrid_hash_dm_(false),
      join_type_(UNKNOWN_JOIN),
      need_mat_(false),
      left_need_sort_(false),
      left_prefix_pos_(0),
      right_need_sort_(false),
      right_prefix_pos_(0),
      left_sort_keys_(),
      right_sort_keys_(),
      merge_directions_(),
      equal_join_conditions_(),
      other_join_conditions_(),
      equal_cond_sel_(-1.0),
      other_cond_sel_(-1.0),
      contain_normal_nl_(false),
      has_none_equal_join_(false),
      can_use_batch_nlj_(false),
      is_naaj_(false),
      is_sna_(false)
    {
    }

    JoinPath(ObJoinOrder* parent,
             const Path* left_path,
             const Path* right_path,
             JoinAlgo join_algo,
             DistAlgo join_dist_algo,
             bool is_slave_mapping,
             ObJoinType join_type,
             bool need_mat = false)
      : Path(parent),
        left_path_(left_path),
        right_path_(right_path),
        join_algo_(join_algo),
        join_dist_algo_(join_dist_algo),
        is_slave_mapping_(is_slave_mapping),
        use_hybrid_hash_dm_(false),
        join_type_(join_type),
        need_mat_(need_mat),
        left_need_sort_(false),
        left_prefix_pos_(0),
        right_need_sort_(false),
        right_prefix_pos_(0),
        left_sort_keys_(),
        right_sort_keys_(),
        merge_directions_(),
        equal_join_conditions_(),
        other_join_conditions_(),
        equal_cond_sel_(-1.0),
        other_cond_sel_(-1.0),
        contain_normal_nl_(false),
        has_none_equal_join_(false),
        can_use_batch_nlj_(false),
        is_naaj_(false),
        is_sna_(false),
        inherit_sharding_index_(-1)
      {
      }
    virtual ~JoinPath() {}
    int assign(const JoinPath &other, common::ObIAllocator *allocator);
    virtual int estimate_cost() override;
    void reuse();
    virtual int re_estimate_cost(EstimateCostInfo &info, double &card, double &cost) override;
    int do_re_estimate_cost(EstimateCostInfo &info, double &card, double &op_cost, double &cost);
    int get_re_estimate_param(EstimateCostInfo &param,
                              EstimateCostInfo &left_param,
                              EstimateCostInfo &right_param,
                              bool re_est_for_op);
    int try_set_batch_nlj_for_right_access_path(bool enable);
    int re_estimate_rows(double left_output_rows, double right_output_rows, double &row_count);
    int cost_nest_loop_join(int64_t join_parallel,
                            double left_output_rows,
                            double left_cost,
                            double right_output_rows,
                            double right_cost,
                            bool re_est_for_op,
                            double &op_cost,
                            double &cost);
    int cost_merge_join(int64_t join_parallel,
                        double left_output_rows,
                        double left_cost,
                        double right_output_rows,
                        double right_cost,
                        bool re_est_for_op,
                        double &op_cost,
                        double &cost);
    int cost_hash_join(int64_t join_parallel,
                      double left_output_rows,
                      double left_cost,
                      double right_output_rows,
                      double right_cost,
                      bool re_est_for_op,
                      double &op_cost,
                      double &cost);
    int compute_join_path_property();
    inline bool is_left_local_order() const
    {
      return NULL != left_path_ && NULL != right_path_ && !left_sort_keys_.empty() &&
             left_path_->is_local_order_ && !is_fully_partition_wise();
    }
    inline bool is_right_local_order() const
    {
      return NULL != right_path_ && NULL != left_path_ && !right_sort_keys_.empty() &&
             right_path_->is_local_order_ && !is_fully_partition_wise();
    }
    inline bool is_left_need_sort() const
    {
      return left_need_sort_ || is_left_local_order();
    }
    inline bool is_right_need_sort() const
    {
      return right_need_sort_ || is_right_local_order();
    }
    inline bool is_left_need_exchange() const {
      return ObPQDistributeMethod::NONE != get_left_dist_method();
    }
    inline ObPQDistributeMethod::Type get_left_dist_method() const
    {
      ObPQDistributeMethod::Type dist_method = ObPQDistributeMethod::NONE;
      if (NULL != left_path_ && NULL != left_path_->get_sharding()) {
        dist_method = ObOptimizerUtil::get_left_dist_method(*left_path_->get_sharding(),
                                                            join_dist_algo_);
      }
      return dist_method;
    }
    inline bool is_right_need_exchange() const {
      return ObPQDistributeMethod::NONE != get_right_dist_method();
    }
    inline ObPQDistributeMethod::Type get_right_dist_method() const
    {
      ObPQDistributeMethod::Type dist_method = ObPQDistributeMethod::NONE;
      if (NULL != right_path_ && NULL != right_path_->get_sharding()) {
        dist_method = ObOptimizerUtil::get_right_dist_method(*right_path_->get_sharding(),
                                                             join_dist_algo_);
      }
      return dist_method;
    }

    inline bool is_fully_partition_wise() const {
      return is_partition_wise() && !exchange_allocated_;
    }
    inline bool is_partition_wise() const
    {
      return (join_dist_algo_ == DistAlgo::DIST_PARTITION_WISE ||
              join_dist_algo_ == DistAlgo::DIST_EXT_PARTITION_WISE) &&
              !is_slave_mapping_;
    }
    inline SlaveMappingType get_slave_mapping_type() const
    {
      SlaveMappingType sm_type = SlaveMappingType::SM_NONE;
      if (!is_slave_mapping_) {
        sm_type = SlaveMappingType::SM_NONE;
      } else if (join_dist_algo_ == DIST_PARTITION_WISE ||
                 join_dist_algo_ == DIST_EXT_PARTITION_WISE) {
        sm_type = SlaveMappingType::SM_PWJ_HASH_HASH;
      } else if (join_dist_algo_ == DIST_PARTITION_NONE ||
                 join_dist_algo_ == DIST_NONE_PARTITION ||
                 join_dist_algo_ == DIST_NONE_HASH ||
                 join_dist_algo_ == DIST_HASH_NONE) {
        sm_type = SlaveMappingType::SM_PPWJ_HASH_HASH;
      } else if (join_dist_algo_ == DIST_BROADCAST_NONE) {
        sm_type = SlaveMappingType::SM_PPWJ_BCAST_NONE;
      } else if (join_dist_algo_ == DIST_NONE_BROADCAST) {
        sm_type = SlaveMappingType::SM_PPWJ_NONE_BCAST;
      } else {
        sm_type = SlaveMappingType::SM_NONE;
      }
      return sm_type;
    }
    bool contain_normal_nl() const { return contain_normal_nl_; }
    void set_contain_normal_nl(bool contain) { contain_normal_nl_ = contain; }
    bool has_none_equal_join() const { return has_none_equal_join_; }
    void set_has_none_equal_join(bool has) { has_none_equal_join_ = has; }
    int check_is_contain_normal_nl();
    virtual int compute_pipeline_info() override;
    virtual int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos) const
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(BUF_PRINTF("<"))) {
      } else if (NULL != left_path_) {
        left_path_->get_name_internal(buf, buf_len, pos);
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(BUF_PRINTF(" -"))) {
      } else {
        switch (join_algo_)
        {
        case INVALID_JOIN_ALGO:
        {
        } break;
        case NESTED_LOOP_JOIN:
        {
          ret = BUF_PRINTF("NL");
        } break;
        case MERGE_JOIN:
        {
          ret = BUF_PRINTF("M");
        } break;
        case HASH_JOIN:
        {
          ret = BUF_PRINTF("H");
        } break;
        default:
          break;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(BUF_PRINTF("-> "))) {
      } else if (NULL != right_path_) {
        right_path_->get_name_internal(buf, buf_len, pos);
      }
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(">");
      }
      return ret;
    }
    static int compute_join_path_parallel_and_server_info(const common::ObAddr &local_server_addr,
                                                          const Path *left_path,
                                                          const Path *right_path,
                                                          const DistAlgo join_dist_algo,
                                                          bool const is_slave_mapping,
                                                          int64_t &parallel,
                                                          int64_t &available_parallel,
                                                          int64_t &server_cnt,
                                                          ObIArray<common::ObAddr> &server_list);
  private:
    int compute_hash_hash_sharding_info();
    int compute_join_path_ordering();
    int compute_join_path_info();
    int compute_join_path_sharding();
    int compute_join_path_plan_type();
    int compute_join_path_parallel_and_server_info();
    int can_use_batch_nlj(bool &use_batch_nlj);
    int can_use_batch_nlj(ObLogPlan *plan, const AccessPath *access_path, bool &use_batch_nlj);
    int can_use_das_batch_nlj(ObLogicalOperator* root, bool &use_batch_nlj);
  public:
    TO_STRING_KV(K_(join_algo),
                 K_(join_dist_algo),
                 K_(is_slave_mapping),
                 K_(join_type),
                 K_(need_mat),
                 K_(left_need_sort),
                 K_(left_prefix_pos),
                 K_(right_need_sort),
                 K_(right_prefix_pos),
                 K_(left_sort_keys),
                 K_(right_sort_keys),
                 K_(merge_directions),
                 K_(equal_join_conditions),
                 K_(other_join_conditions),
                 K_(join_filter_infos),
                 K_(exchange_allocated),
                 K_(contain_normal_nl),
                 K_(can_use_batch_nlj),
                 K_(is_naaj),
                 K_(is_sna),
                 K_(inherit_sharding_index));
  public:
    const Path *left_path_;
    const Path *right_path_;
    JoinAlgo join_algo_; // e.g., merge, hash, nested loop
    DistAlgo join_dist_algo_; // e.g, partition_wise_join, repartition, hash-hash
    bool is_slave_mapping_; // whether should enable slave mapping
    bool use_hybrid_hash_dm_; // if use hybrid hash distribution method for hash-hash dm
    ObJoinType join_type_;
    bool need_mat_;
    // for merge joins only
    bool left_need_sort_;
    int64_t left_prefix_pos_;
    bool right_need_sort_;
    int64_t right_prefix_pos_;
    common::ObSEArray<OrderItem, 4, common::ModulePageAllocator, true> left_sort_keys_;
    common::ObSEArray<OrderItem, 4, common::ModulePageAllocator, true> right_sort_keys_;
    common::ObSEArray< ObOrderDirection, 4, common::ModulePageAllocator, true> merge_directions_;
    // for all types of join
    common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> equal_join_conditions_;
    common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> other_join_conditions_;
    common::ObSEArray<JoinFilterInfo, 2, common::ModulePageAllocator, true> join_filter_infos_;
    // for hash join only, used to simplify the re-estimate phase
    double equal_cond_sel_;
    double other_cond_sel_;
    bool contain_normal_nl_;
    bool has_none_equal_join_;
    bool can_use_batch_nlj_;
    bool is_naaj_; // is null aware anti join
    bool is_sna_; // is single null aware anti join
    //Used to indicate which child node the current sharding inherits from
    int64_t inherit_sharding_index_;
  private:
      DISALLOW_COPY_AND_ASSIGN(JoinPath);
  };

  class SubQueryPath : public Path
  {
  public:
    SubQueryPath()
      : Path(NULL),
        subquery_id_(common::OB_INVALID_ID),
        root_(NULL) {}
    SubQueryPath(ObLogicalOperator* root)
      : Path(NULL),
        subquery_id_(common::OB_INVALID_ID),
        root_(root) {}
    virtual ~SubQueryPath() { }
    int assign(const SubQueryPath &other, common::ObIAllocator *allocator);
    virtual int estimate_cost() override;
    virtual int re_estimate_cost(EstimateCostInfo &info, double &card, double &cost) override;
    virtual int compute_pipeline_info() override;
    virtual int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos) const
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(BUF_PRINTF("@sub_"))) {
      } else if (OB_FAIL(BUF_PRINTF("%lu", subquery_id_))) {
      }
      return ret;
    }
  public:
    uint64_t subquery_id_;//该subquery所在TableItem的table_id_
    ObLogicalOperator* root_;

  private:
      DISALLOW_COPY_AND_ASSIGN(SubQueryPath);
  };

  class FunctionTablePath : public Path
  {
  public:
    FunctionTablePath()
      : Path(NULL),
        table_id_(OB_INVALID_ID),
        value_expr_(NULL) {}
    virtual ~FunctionTablePath() { }
    int assign(const FunctionTablePath &other, common::ObIAllocator *allocator);
    virtual int estimate_cost() override;
    virtual int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos) const
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(BUF_PRINTF("@function_"))) {
      } else if (OB_FAIL(BUF_PRINTF("%lu", table_id_))) {
      }
      return ret;
    }
  public:
    uint64_t table_id_;
    ObRawExpr* value_expr_;
  private:
      DISALLOW_COPY_AND_ASSIGN(FunctionTablePath);
  };

  class JsonTablePath : public Path
  {
  public:
    JsonTablePath()
      : Path(NULL),
        table_id_(OB_INVALID_ID),
        value_expr_(NULL),
        column_param_default_exprs_() {}
    virtual ~JsonTablePath() {}
    int assign(const JsonTablePath &other, common::ObIAllocator *allocator);
    virtual int estimate_cost() override;
    virtual int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos) const
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(BUF_PRINTF("@json_table_"))) {
      } else if (OB_FAIL(BUF_PRINTF("%lu", table_id_))) {
      }
      return ret;
    }
  public:
    uint64_t table_id_;
    ObRawExpr* value_expr_;
    common::ObSEArray<ObColumnDefault, 1, common::ModulePageAllocator, true> column_param_default_exprs_;
  private:
      DISALLOW_COPY_AND_ASSIGN(JsonTablePath);
  };

  class TempTablePath : public Path
  {
  public:
    TempTablePath()
      : Path(NULL),
        table_id_(OB_INVALID_ID),
        temp_table_id_(OB_INVALID_ID),
        root_(NULL) { }
    virtual ~TempTablePath() { }
    int assign(const TempTablePath &other, common::ObIAllocator *allocator);
    virtual int estimate_cost() override;
    virtual int re_estimate_cost(EstimateCostInfo &info, double &card, double &cost) override;
    int compute_sharding_info();
    int compute_path_ordering();
    virtual int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos) const
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(BUF_PRINTF("@temp_"))) {
      } else if (OB_FAIL(BUF_PRINTF("%lu", table_id_))) {
      }
      return  ret;
    }
  public:
    uint64_t table_id_;
    uint64_t temp_table_id_;
    ObLogicalOperator *root_;
  private:
      DISALLOW_COPY_AND_ASSIGN(TempTablePath);
  };


  class CteTablePath : public Path
  {
  public:
    CteTablePath()
      : Path(NULL),
        table_id_(OB_INVALID_ID),
        ref_table_id_(OB_INVALID_ID) {}
    virtual ~CteTablePath() { }
    int assign(const CteTablePath &other, common::ObIAllocator *allocator);
    virtual int estimate_cost() override;
    virtual int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos) const
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(BUF_PRINTF("@cte_"))) {
      } else if (OB_FAIL(BUF_PRINTF("%lu", table_id_))) {
      }
      return ret;
    }
  public:
    uint64_t table_id_;
    uint64_t ref_table_id_;
  private:
      DISALLOW_COPY_AND_ASSIGN(CteTablePath);
  };

  class ValuesTablePath : public Path
  {
  public:
    ValuesTablePath()
      : Path(NULL),
        table_id_(OB_INVALID_ID),
        table_def_(NULL) {}
    virtual ~ValuesTablePath() { }
    int assign(const ValuesTablePath &other, common::ObIAllocator *allocator);
    virtual int estimate_cost() override;
    virtual int estimate_row_count();
    virtual int get_name_internal(char *buf, const int64_t buf_len, int64_t &pos) const
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(BUF_PRINTF("@values_"))) {
      } else if (OB_FAIL(BUF_PRINTF("%lu", table_id_))) {
      }
      return ret;
    }
  public:
    uint64_t table_id_;
    ObValuesTableDef *table_def_;
  private:
      DISALLOW_COPY_AND_ASSIGN(ValuesTablePath);
  };

  struct ObRowCountEstTask
  {
    ObRowCountEstTask() : est_arg_(NULL)
    {}

    ObAddr addr_;
    ObBitSet<> path_id_set_;
    obrpc::ObEstPartArg *est_arg_;

    TO_STRING_KV(K_(addr),
                 K_(path_id_set));
  };

struct InnerPathInfo {
  InnerPathInfo() :
        join_conditions_(),
        inner_paths_(),
        table_opt_info_(),
        force_inner_nl_(false) {}
  virtual ~InnerPathInfo() {}
  TO_STRING_KV(K_(join_conditions),
               K_(inner_paths),
               K_(force_inner_nl));

  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> join_conditions_;
  common::ObSEArray<Path *, 8, common::ModulePageAllocator, true> inner_paths_;
  BaseTableOptInfo table_opt_info_;
  bool force_inner_nl_; //force generation of inner path, ignoring range check
};
typedef  common::ObSEArray<InnerPathInfo, 8, common::ModulePageAllocator, true> InnerPathInfos;

struct NullAwareAntiJoinInfo {
  NullAwareAntiJoinInfo() : is_naaj_(false), is_sna_(false),
                            left_side_not_null_(false), right_side_not_null_(false) {}
  ~NullAwareAntiJoinInfo() {}
  TO_STRING_KV(K_(is_naaj), K_(is_sna), K_(left_side_not_null), K_(right_side_not_null), K_(expr_constraints));
  void set_is_sna(const ObJoinType &join_type, const bool is_reverse_path)
  {
    if (is_naaj_) {
      if (is_reverse_path) {
        is_sna_ = ((LEFT_ANTI_JOIN == join_type && left_side_not_null_)
                            || (RIGHT_ANTI_JOIN == join_type && right_side_not_null_));
      } else {
        is_sna_ = ((LEFT_ANTI_JOIN == join_type && right_side_not_null_)
                            || (RIGHT_ANTI_JOIN == join_type && left_side_not_null_));
      }
    }
    return;
  }
  bool is_naaj_;
  bool is_sna_;
  bool left_side_not_null_;
  bool right_side_not_null_;
  ObSEArray<ObExprConstraint, 2> expr_constraints_;
};

  class ObJoinOrder
  {
  public:
    // used for heuristic index selection
    static const int64_t TABLE_HEURISTIC_UNIQUE_KEY_RANGE_THRESHOLD = 10000;
    static const int64_t PRUNING_ROW_COUNT_THRESHOLD = 1000;

    struct PathHelper {
      PathHelper()
      : is_inner_path_(false),
        force_inner_nl_(false),
        child_stmt_(NULL),
        pushdown_filters_(),
        filters_(),
        subquery_exprs_(),
        inner_paths_(),
        table_opt_info_(NULL),
        est_method_(EST_INVALID)
      {}

      bool is_inner_path_;
      bool force_inner_nl_;
      ObSelectStmt *child_stmt_;
      // when generate inner access path, save all pushdown filters
      // when generate subquery path, save all pushdown filters after rename
      common::ObSEArray<ObRawExpr *, 4> pushdown_filters_;
      // when generate inner access path, save base table filters
      // when generate subquery path, save filters can not pushdown
      common::ObSEArray<ObRawExpr *, 4> filters_;
      common::ObSEArray<ObRawExpr *, 4> subquery_exprs_;
      common::ObSEArray<Path*, 8> inner_paths_;  //生成的inner path
      BaseTableOptInfo *table_opt_info_;
      ObSEArray<ObPCParamEqualInfo, 4> equal_param_constraints_;
      ObSEArray<ObPCConstParamInfo, 4> const_param_constraints_;

      ObSEArray<ObExprConstraint, 4> expr_constraints_;
      ObBaseTableEstMethod est_method_;
    };

    struct DeducedExprInfo {
      DeducedExprInfo() :
      deduced_expr_(NULL),
      deduced_from_expr_(NULL),
      is_precise_(false),
      const_param_constraints_() {}

      ObRawExpr * deduced_expr_;
      ObRawExpr * deduced_from_expr_;
      bool is_precise_;
      common::ObSEArray<ObPCConstParamInfo, 2, common::ModulePageAllocator, true> const_param_constraints_;

      int assign(const DeducedExprInfo& other);
      TO_STRING_KV(
        K_(deduced_expr),
        K_(deduced_from_expr),
        K_(is_precise)
      );
    };

    ObJoinOrder(common::ObIAllocator *allocator,
                ObLogPlan *plan,
                PathType type)
    : allocator_(allocator),
      plan_(plan),
      type_(type),
      table_id_(common::OB_INVALID_ID),
      table_set_(),
      output_table_set_(),
      output_rows_(-1.0),
      output_row_size_(-1.0),
      anti_or_semi_match_sel_(1.0),
      table_partition_info_(NULL),
      sharding_info_(NULL),
      table_meta_info_(common::OB_INVALID_ID),
      join_info_(NULL),
      used_conflict_detectors_(),
      restrict_info_set_(),
      interesting_paths_(),
      is_at_most_one_row_(false),
      output_equal_sets_(),
      output_const_exprs_(),
      table_opt_info_(),
      available_access_paths_(),
      diverse_path_count_(0),
      fd_item_set_(),
      candi_fd_item_set_(),
      not_null_columns_(),
      inner_path_infos_(),
      cnt_rownum_(false),
      total_path_num_(0)
    {
    }
    virtual ~ObJoinOrder();

    int skyline_prunning_index(const uint64_t table_id,
                               const uint64_t base_table_id,
                               const ObDMLStmt *stmt,
                               const bool do_prunning,
                               const ObIndexInfoCache &index_info_cache,
                               const common::ObIArray<uint64_t> &valid_index_ids,
                               common::ObIArray<uint64_t> &skyline_index_ids,
                               ObIArray<ObRawExpr *> &restrict_infos);

    int pruning_unstable_access_path(BaseTableOptInfo *table_opt_info,
                                     ObIArray<AccessPath *> &access_paths);
    int try_pruning_base_table_access_path(ObIArray<AccessPath*> &access_paths,
                                           ObIArray<uint64_t> &unstable_index_id);

    int cal_dimension_info(const uint64_t table_id,
                           const uint64_t data_table_id,
                           const uint64_t index_table_id,
                           const ObDMLStmt *stmt,
                           ObIndexSkylineDim &index_dim,
                           const ObIndexInfoCache &index_info_cache,
                           ObIArray<ObRawExpr *> &restrict_infos);

    int fill_index_info_entry(const uint64_t table_id,
                              const uint64_t base_table_id,
                              const uint64_t index_id,
                              IndexInfoEntry *&index_entry,
                              PathHelper &helper);
    int fill_index_info_cache(const uint64_t table_id,
                              const uint64_t base_table_id,
                              const common::ObIArray<uint64_t> &valid_index_ids,
                              ObIndexInfoCache &index_info_cache,
                              PathHelper &helper);

    int fill_opt_info_index_name(const uint64_t table_id,
                                 const uint64_t base_table_id,
                                 ObIArray<uint64_t> &available_index_id,
                                 ObIArray<uint64_t> &unstable_index_id,
                                 BaseTableOptInfo *table_opt_info);

    int extract_used_columns(const uint64_t table_id,
                            const uint64_t ref_table_id,
                            bool only_normal_ref_expr,
                            bool consider_rowkey,
                            ObIArray<uint64_t> &column_ids,
                            ObIArray<ColumnItem> &columns);

    int get_simple_index_info(const uint64_t table_id,
                              const uint64_t ref_table_id,
                              const uint64_t index_id,
                              bool &is_unique_index,
                              bool &is_index_back,
                              bool &is_global_index);

    int get_matched_inv_index_tid(ObMatchFunRawExpr *match_expr,
                                  uint64_t ref_table_id,
                                  uint64_t &inv_idx_tid);

    inline ObTablePartitionInfo *get_table_partition_info() { return table_partition_info_; }

    int param_funct_table_expr(ObRawExpr* &function_table_expr,
                               ObIArray<ObExecParamRawExpr *> &nl_params,
                               ObIArray<ObRawExpr*> &subquery_exprs);

    int param_values_table_expr(ObIArray<ObRawExpr*> &values_vector,
                                ObIArray<ObExecParamRawExpr *> &nl_params,
                                ObIArray<ObRawExpr*> &subquery_exprs);

    int param_json_table_expr(ObRawExpr* &json_table_expr,
                              ObIArray<ObExecParamRawExpr *> &nl_params,
                              ObIArray<ObRawExpr*> &subquery_exprs);
    int generate_json_table_default_val(ObIArray<ObExecParamRawExpr *> &nl_param,
                                        ObIArray<ObRawExpr *> &subquery_exprs,
                                        ObRawExpr*& default_expr);
    /**
     * 为本节点增加一条路径，代价竞争过程在这里实现
     * @param path
     * @return
     */
    int add_path(Path* path);
    int add_recycled_paths(Path* path);
    int compute_path_relationship(const Path &first_path,
                                  const Path &second_path,
                                  DominateRelation &relation);

    int compute_pipeline_relationship(const Path &first_path,
                                      const Path &second_path,
                                      DominateRelation &relation);

    int estimate_size_for_base_table(PathHelper &helper,
                                    ObIArray<AccessPath *> &access_paths);

    int estimate_size_and_width_for_join(const ObJoinOrder* lefttree,
                                         const ObJoinOrder* righttree,
                                         const ObJoinType join_type);

    int estimate_size_and_width_for_subquery(uint64_t table_id,
                                             ObLogicalOperator *root);

    int estimate_size_and_width_for_access(PathHelper &helper,
                                           ObIArray<AccessPath *> &access_paths);

    int est_join_width();

    int compute_equal_set_for_join(const ObJoinOrder* left_tree,
                                   const ObJoinOrder* right_tree,
                                   const ObJoinType join_type);

    int compute_equal_set_for_subquery(uint64_t table_id, ObLogicalOperator *root);

    int generate_const_predicates_from_view(const ObDMLStmt *stmt,
                                            const ObSelectStmt *child_stmt,
                                            uint64_t table_id,
                                            ObIArray<ObRawExpr *> &preds);

    int compute_const_exprs_for_join(const ObJoinOrder* left_tree,
                                      const ObJoinOrder* right_tree,
                                      const ObJoinType join_type);

    int compute_const_exprs_for_subquery(uint64_t table_id, ObLogicalOperator *root);

    static int convert_subplan_scan_order_item(ObLogPlan &plan,
                                               ObLogicalOperator &subplan_root,
                                               const uint64_t table_id,
                                               ObIArray<OrderItem> &output_order);

    static int convert_subplan_scan_sharding_info(ObLogPlan &plan,
                                                  ObLogicalOperator &subplan_root,
                                                  const uint64_t table_id,
                                                  ObShardingInfo *&output_strong_sharding,
                                                  ObIArray<ObShardingInfo*> &output_weak_sharding);

    static int convert_subplan_scan_sharding_info(ObLogPlan &plan,
                                                  ObLogicalOperator &subplan_root,
                                                  const uint64_t table_id,
                                                  bool is_strong,
                                                  ObShardingInfo *input_sharding,
                                                  ObShardingInfo *&output_sharding);

    int compute_table_meta_info(const uint64_t table_id, const uint64_t ref_table_id);

    int fill_path_index_meta_info(const uint64_t table_id,
                                  const uint64_t ref_table_id,
                                  ObIArray<AccessPath *> &access_paths);

    // 用于更新统计信息
    int init_est_sel_info_for_access_path(const uint64_t table_id,
                                          const uint64_t ref_table_id,
                                          const share::schema::ObTableSchema &table_schema);

    int init_est_info_for_index(const uint64_t index_id,
                                ObIndexMetaInfo &meta_info,
                                ObTablePartitionInfo *table_partition_info,
                                const share::schema::ObTableSchema &index_schema,
                                bool &has_opt_stat);

    int init_est_sel_info_for_subquery(const uint64_t table_id,
                                       ObLogicalOperator *root);

    int check_use_global_stat(const uint64_t ref_table_id,
                              const share::schema::ObTableSchema &table_schema,
                              ObIArray<int64_t> &all_used_parts,
                              ObIArray<common::ObTabletID> &all_used_tablets,
                              bool &can_use);

    inline double get_output_rows() const {return output_rows_;}

    inline void set_output_rows(double rows) { output_rows_ = rows;}

    inline common::ObIArray<ObConflictDetector*>& get_conflict_detectors() {return used_conflict_detectors_;}
    inline const common::ObIArray<ObConflictDetector*>& get_conflict_detectors() const {return used_conflict_detectors_;}
    int merge_conflict_detectors(ObJoinOrder *left_tree,
                                 ObJoinOrder *right_tree,
                                 const common::ObIArray<ObConflictDetector*>& detectors);
    inline JoinInfo* get_join_info() {return join_info_;}
    inline const JoinInfo* get_join_info() const {return join_info_;}

    inline ObRelIds& get_tables() {return table_set_;}
    inline const ObRelIds& get_tables() const { return table_set_; }

    inline ObRelIds& get_output_tables() {return output_table_set_;}
    inline const ObRelIds& get_output_tables() const { return output_table_set_; }

    inline common::ObIAllocator *get_allocator() { return allocator_; }

    inline common::ObIArray<ObRawExpr*>& get_restrict_infos() {return restrict_info_set_;}

    inline common::ObIArray<Path*>& get_interesting_paths() {return interesting_paths_;}
    inline const common::ObIArray<Path*>& get_interesting_paths() const {return interesting_paths_;}

    inline void set_type(PathType type) {type_ = type;}

    inline PathType get_type() {return type_;}
    inline PathType get_type() const { return type_;}

    inline ObLogPlan* get_plan(){return plan_;}
    inline const ObLogPlan* get_plan() const {return plan_;}

    inline uint64_t get_table_id() const {return table_id_;}

    const EqualSets& get_output_equal_sets() const { return output_equal_sets_; }
    EqualSets& get_output_equal_sets() { return output_equal_sets_; }

    inline common::ObIArray<ObRawExpr *> &get_output_const_exprs() { return output_const_exprs_; }
    inline const common::ObIArray<ObRawExpr *> &get_output_const_exprs() const { return output_const_exprs_; }

    inline void set_is_at_most_one_row(bool is_at_most_one_row) { is_at_most_one_row_ = is_at_most_one_row; }
    inline bool get_is_at_most_one_row() const { return is_at_most_one_row_; }

    int alloc_join_path(JoinPath *&join_path);
    int create_access_paths(const uint64_t table_id,
                            const uint64_t ref_table_id,
                            PathHelper &helper,
                            ObIArray<AccessPath *> &access_paths,
                            ObIndexInfoCache &index_info_cache);

    int create_one_access_path(const uint64_t table_id,
                               const uint64_t ref_id,
                               const uint64_t index_id,
                               const ObIndexInfoCache &index_info_cache,
                               PathHelper &helper,
                               AccessPath *&ap,
                               bool use_das,
                               bool use_column_store,
                               OptSkipScanState use_skip_scan);

    int init_sample_info_for_access_path(AccessPath *ap,
                                         const uint64_t table_id);

    int init_filter_selectivity(ObCostTableScanInfo &est_cost_info);

    int init_column_store_est_info(const uint64_t table_id,
                                   const uint64_t ref_id,
                                   ObCostTableScanInfo &est_cost_info);

    int init_column_store_est_info_with_filter(const uint64_t table_id,
                                                    ObCostTableScanInfo &est_cost_info,
                                                    const OptTableMetas& table_opt_meta,
                                                    ObIArray<ObRawExpr*> &filters,
                                                    ObIArray<ObCostColumnGroupInfo> &column_group_infos,
                                                    ObSqlBitSet<> &used_column_ids,
                                                    FilterCompare &filter_compare,
                                                    const bool use_filter_sel);

    int init_column_store_est_info_with_other_column(const uint64_t table_id,
                                                    ObCostTableScanInfo &est_cost_info,
                                                    const OptTableMetas& table_opt_meta,
                                                    ObSqlBitSet<> &used_column_ids);

    int will_use_das(const uint64_t table_id,
                     const uint64_t ref_id,
                     const uint64_t index_id,
                     const ObIndexInfoCache &index_info_cache,
                     PathHelper &helper,
                     bool &create_das_path,
                     bool &create_basic_path);

    int will_use_skip_scan(const uint64_t table_id,
                           const uint64_t ref_id,
                           const uint64_t index_id,
                           const ObIndexInfoCache &index_info_cache,
                           PathHelper &helper,
                           ObSQLSessionInfo *session_info,
                           OptSkipScanState &use_skip_scan);

    int get_access_path_ordering(const uint64_t table_id,
                                 const uint64_t ref_table_id,
                                 const uint64_t index_id,
                                 common::ObIArray<ObRawExpr *> &index_keys,
                                 common::ObIArray<ObRawExpr *> &ordering,
                                 ObOrderDirection &direction,
                                 const bool is_index_back);

    int get_index_scan_direction(const ObIArray<ObRawExpr *> &keys,
                                 const ObDMLStmt *stmt,
                                 const EqualSets &equal_sets,
                                 ObOrderDirection &index_direction);

    int get_direction_in_order_by(const ObIArray<OrderItem> &order_by,
                                  const ObIArray<ObRawExpr *> &index_keys,
                                  const int64_t index_start_offset,
                                  const EqualSets &equal_sets,
                                  const ObIArray<ObRawExpr *> &const_exprs,
                                  ObOrderDirection &direction,
                                  int64_t &order_match_count);

    /**
     * Extract query range for a certain table(index) given a list of predicates
     */
    int extract_preliminary_query_range(const common::ObIArray<ColumnItem> &range_columns,
                                        const common::ObIArray<ObRawExpr*> &predicates,
                                        ObIArray<ObExprConstraint> &expr_constraints,
                                        int64_t table_id,
                                        ObQueryRange* &range,
                                        int64_t index_id);

    int check_enable_better_inlist(int64_t table_id, bool &enable);

    int get_candi_range_expr(const ObIArray<ColumnItem> &range_columns,
                            const ObIArray<ObRawExpr*> &predicates,
                            ObIArray<ObRawExpr*> &range_predicates);

    int calculate_range_expr_cost(ObIArray<CandiRangeExprs*> &sorted_predicates,
                                  ObIArray<ObRawExpr*> &range_exprs,
                                  int64_t range_column_count,
                                  int64_t range_count,
                                  double &cost);

    int sort_predicate_by_index_column(const ObIArray<ColumnItem> &range_columns,
                                       const ObIArray<ObRawExpr*> &predicates,
                                       ObIArray<CandiRangeExprs*> &sort_exprs,
                                       bool &has_in_pred);

    int is_eq_or_in_range_expr(ObRawExpr* expr,
                               int64_t &column_id,
                               bool &is_in_expr,
                               bool &is_valid);

    int get_range_filter(ObIArray<CandiRangeExprs*> &sort_exprs,
                         ObIArray<ObRawExpr*> &range_exprs,
                         ObIArray<ObRawExpr*> &filters);

    int extract_geo_preliminary_query_range(const ObIArray<ColumnItem> &range_columns,
                                              const ObIArray<ObRawExpr*> &predicates,
                                              const ColumnIdInfoMap &column_schema_info,
                                              ObQueryRange *&query_range);

    int extract_multivalue_preliminary_query_range(const ObIArray<ColumnItem> &range_columns,
                                                  const ObIArray<ObRawExpr*> &predicates,
                                                  ObQueryRange *&query_range);

    int extract_geo_schema_info(const uint64_t table_id,
                                const uint64_t index_id,
                                ObWrapperAllocator &wrap_allocator,
                                ColumnIdInfoMapAllocer &map_alloc,
                                ColumnIdInfoMap &geo_columnInfo_map);

    int check_expr_match_first_col(const ObRawExpr * qual,
                                   const common::ObIArray<ObRawExpr *>& keys,
                                   bool &match);

    int extract_filter_column_ids(const ObIArray<ObRawExpr*> &quals,
                                  const bool is_data_table,
                                  const share::schema::ObTableSchema &index_schema,
                                  ObIArray<uint64_t> &filter_column_ids);

    int check_expr_overlap_index(const ObRawExpr* qual,
                                 const common::ObIArray<ObRawExpr*>& keys,
                                 bool &overlap);

    int check_exprs_overlap_index(const common::ObIArray<ObRawExpr*>& quals,
                                  const common::ObIArray<ObRawExpr*>& keys,
                                  bool &match);
    int check_exprs_overlap_gis_index(const ObIArray<ObRawExpr*>& quals,
                                      const ObIArray<ObRawExpr*>& keys,
                                      bool &match);

    int check_exprs_overlap_multivalue_index(const uint64_t table_id,
                                             const uint64_t index_table_id,
                                             const ObIArray<ObRawExpr*>& quals,
                                             const ObIArray<ObRawExpr*>& keys,
                                             bool &match);

    /**
     * 判断连接条件是否匹配索引前缀
     * @keys 索引列
     * @inner_join_infos 内连接等值条件
     * @outer_join_info 外连接条件
     * @match_prefix_count 匹配索引前缀的长度
     * @sort_match 是否匹配
     */
    int is_join_match(const ObIArray<OrderItem> &ordering,
                      int64_t &match_prefix_count,
                      bool &sort_match);

    // fast check, just return bool result
    int is_join_match(const ObIArray<OrderItem> &ordering,
                      bool &sort_match);

    /**
     * 检查是否是interesting order
     * @keys 索引列
     * @stmt
     * @interest_column_ids 匹配的索引列的id
     */
    int check_all_interesting_order(const ObIArray<OrderItem> &ordering,
                                    const ObDMLStmt *stmt,
                                    int64_t &max_prefix_count,
                                    int64_t &interesting_order_info);

    int check_all_interesting_order(const ObIArray<OrderItem> &ordering,
                                    const ObDMLStmt *stmt,
                                    int64_t &interesting_order_info);

    int extract_interesting_column_ids(const ObIArray<ObRawExpr*> &keys,
                                       const int64_t &max_prefix_count,
                                       ObIArray<uint64_t> &interest_column_ids,
                                       ObIArray<bool> &const_column_info);


    int check_and_extract_filter_column_ids(const ObIArray<ObRawExpr *> &index_keys,
                                            ObIArray<uint64_t> &restrict_ids);

    /*
     * 看能否在索引前缀上抽取query range
     * @table_id
     * @index_table_id
     * @index_keys 索引列
     * @prefix_range_ids 索引前缀id
     * */
    int check_and_extract_query_range(const uint64_t table_id,
                                      const uint64_t index_table_id,
                                      const IndexInfoEntry &index_info_entry,
                                      const ObIArray<ObRawExpr*> &index_keys,
                                      const ObIndexInfoCache &index_info_cache,
                                      bool &contain_always_false,
                                      common::ObIArray<uint64_t> &prefix_range_ids,
                                      ObIArray<ObRawExpr *> &restrict_infos);

    /**
     * 生成所有的一级表（包括基表，subqueryscan，和用户指定的join），
     * 这一步只生成相应的ObJoinOrder，不生成具体路径——因为条件还没有下推。
     * @param jo
     * @param from_item
     * @return
     */
    int init_base_join_order(const TableItem *table_item);

    int generate_base_paths();

    int generate_normal_base_table_paths();

    int generate_base_table_paths(PathHelper &helper);

    int compute_base_table_property(uint64_t table_id,
                                    uint64_t ref_table_id);

    int extract_necessary_pushdown_quals(ObIArray<ObRawExpr *> &candi_quals,
                                         ObIArray<ObRawExpr *> &necessary_pushdown_quals,
                                         ObIArray<ObRawExpr *> &unnecessary_pushdown_quals);

    /**
     * @brief generate_subquery_path
     * 生成子查询路径
     * 如果pushdown_filters为空，则是生成普通的子查询路径，
     * 如果pushdown_filters不为空，则是生成条件下推的子查询路径
     */

    int generate_normal_subquery_paths();

    int generate_subquery_paths(PathHelper &helper);

    // generate physical property for each subquery path, including ordering, sharding
    int compute_subquery_path_property(const uint64_t table_id,
                                       ObLogicalOperator *root,
                                       Path *path);

    // generate logical property for each subquery, including const exprs, fd item sets
    int compute_subquery_property(const uint64_t table_id,
                                   ObLogicalOperator *root);

    int estimate_size_for_inner_subquery_path(double root_card,
                                              const ObIArray<ObRawExpr*> &filters,
                                              double &output_card);

    int estimate_size_and_width_for_fake_cte(uint64_t table_id, ObSelectLogPlan *nonrecursive_plan);
    int create_one_cte_table_path(const TableItem* table_item,
                                  ObShardingInfo * sharding);
    int generate_cte_table_paths();
    int generate_function_table_paths();
    int generate_json_table_paths();
    int generate_values_table_paths();
    int generate_temp_table_paths();

    int compute_sharding_info_for_base_paths(ObIArray<AccessPath *> &access_paths, ObIndexInfoCache &index_info_cache);

    int set_sharding_info_for_base_path(ObIArray<AccessPath *> &access_paths,
                                            ObIndexInfoCache &index_info_cache,
                                            const int64_t cur_idx);
    int compute_sharding_info_with_part_info(ObTableLocationType location_type,
                                            ObTablePartitionInfo* table_partition_info,
                                            ObShardingInfo *&sharding_info);
    int get_sharding_info_from_available_access_paths(const uint64_t table_id,
                                                      const uint64_t ref_table_id,
                                                      const uint64_t index_id,
                                                      bool is_global_index,
                                                      ObShardingInfo *&sharding_info) const;
    int get_table_partition_info_from_available_access_paths(const uint64_t table_id,
                                                            const uint64_t ref_table_id,
                                                            const uint64_t index_id,
                                                            ObTablePartitionInfo *&table_part_info);
    int compute_base_table_path_plan_type(AccessPath *access_path);
    int compute_base_table_path_ordering(AccessPath *access_path);
    int compute_parallel_and_server_info_for_base_paths(ObIArray<AccessPath *> &access_paths);
    int get_base_path_table_dop(uint64_t index_id, int64_t &parallel);
    int compute_access_path_parallel(ObIArray<AccessPath *> &access_paths,
                                     int64_t &parallel);
    int get_random_parallel(const int64_t parallel_degree_limit, int64_t &parallel);
    int get_parallel_from_available_access_paths(int64_t &parallel) const;
    int compute_base_table_parallel_and_server_info(const OpParallelRule op_parallel_rule,
                                                    const int64_t parallel,
                                                    AccessPath *path);
    int get_explicit_dop_for_path(const uint64_t index_id, int64_t &parallel);
    int prune_paths_due_to_parallel(ObIArray<AccessPath *> &access_paths);
    /**
     * 根据输入的左右树，生成连接之后的树。
     * 此过程会生成一个ObJoinOrder输出，ObJoinOrder中包含若干个JoinOrder，每个JoinOrder是一个确定的连接方法。
     * 一般情况下left_tree会比right_tree包含的表多，但是不排除相反的情况（right_tree是一个join或subquery），
     * 因为我们要尽量生成左深树，当right_tree包含的表多时，交换输入的左右分支。
     * 如果左右分支的表数相等，我们同时生成两种情况。
     * @param left_tree
     * @param right_tree
     * @param join_info
     * @param detectors
     * @return
     */
    int init_join_order(const ObJoinOrder* left_tree,
                        const ObJoinOrder* right_tree,
                        const JoinInfo* join_info,
                        const common::ObIArray<ObConflictDetector*>& detectors);
    int compute_join_property(const ObJoinOrder *left_tree,
                              const ObJoinOrder *right_tree,
                              const JoinInfo *join_info);
    int generate_join_paths(const ObJoinOrder &left_tree,
                            const ObJoinOrder &right_tree,
                            const JoinInfo &join_info,
                            bool force_ordered = false);
    int inner_generate_join_paths(const ObJoinOrder &left_tree,
                                     const ObJoinOrder &right_tree,
                                     const EqualSets &equal_sets,
                                     const ObIArray<ObSEArray<Path*, 16>> &left_paths,
                                     const ObIArray<ObSEArray<Path*, 16>> &right_paths,
                                     const ObIArray<ObRawExpr*> &on_conditions,
                                     const ObIArray<ObRawExpr*> &where_conditions,
                                     const ValidPathInfo &path_info,
                                     const ValidPathInfo &reverse_path_info);
    int classify_paths_based_on_sharding(const ObIArray<Path*> &input_paths,
                                         const EqualSets &equal_sets,
                                         ObIArray<ObSEArray<Path*, 16>> &ouput_list);
    int generate_nl_paths(const EqualSets &equal_sets,
                          const ObIArray<ObSEArray<Path*, 16>> &left_paths,
                          const ObIArray<ObSEArray<Path*, 16>> &right_paths,
                          const ObIArray<ObRawExpr*> &left_join_keys,
                          const ObIArray<ObRawExpr*> &right_join_keys,
                          const ObIArray<bool> &null_safe_info,
                          const common::ObIArray<ObRawExpr*> &on_conditions,
                          const common::ObIArray<ObRawExpr*> &where_conditions,
                          const ValidPathInfo &path_info,
                          const bool has_non_nl_path,
                          const bool has_equal_cond);

    int create_plan_for_inner_path(Path *path);

    int create_subplan_filter_for_join_path(Path *path,
                                            ObIArray<ObRawExpr*> &subquery_filters);

    int check_valid_for_inner_path(const ObIArray<ObRawExpr*> &join_conditions,
                                   const ValidPathInfo &path_info,
                                   const ObJoinOrder &right_tree,
                                   bool &is_valid);

    int generate_inner_nl_paths(const EqualSets &equal_sets,
                                const ObIArray<Path*> &left_paths,
                                Path *right_path,
                                const ObIArray<ObRawExpr*> &left_join_keys,
                                const ObIArray<ObRawExpr*> &right_join_keys,
                                const ObIArray<bool> &null_safe_info,
                                const ObIArray<ObRawExpr*> &on_conditions,
                                const ObIArray<ObRawExpr*> &where_conditions,
                                const ValidPathInfo &path_info,
                                const bool has_equal_cond);

    int generate_normal_nl_paths(const EqualSets &equal_sets,
                                 const ObIArray<Path*> &left_paths,
                                 Path *right_path,
                                 const ObIArray<ObRawExpr*> &left_join_keys,
                                 const ObIArray<ObRawExpr*> &right_join_keys,
                                 const ObIArray<bool> &null_safe_info,
                                 const common::ObIArray<ObRawExpr*> &on_conditions,
                                 const common::ObIArray<ObRawExpr*> &where_conditions,
                                 const ValidPathInfo &path_info,
                                 const bool has_equal_cond);

    int generate_hash_paths(const EqualSets &equal_sets,
                            const ObIArray<ObSEArray<Path*, 16>> &left_paths,
                            const ObIArray<ObSEArray<Path*, 16>> &right_paths,
                            const ObIArray<ObRawExpr*> &left_join_keys,
                            const ObIArray<ObRawExpr*> &right_join_keys,
                            const ObIArray<bool> &null_safe_info,
                            const common::ObIArray<ObRawExpr*> &join_conditions,
                            const common::ObIArray<ObRawExpr*> &join_filters,
                            const common::ObIArray<ObRawExpr*> &filters,
                            const double equal_cond_sel,
                            const double other_cond_sel,
                            const ValidPathInfo &path_info,
                            const NullAwareAntiJoinInfo &naaj_info);

    int generate_mj_paths(const EqualSets &equal_sets,
                          const ObIArray<ObSEArray<Path*, 16>> &left_paths,
                          const ObIArray<ObSEArray<Path*, 16>> &right_paths,
                          const ObIArray<ObSEArray<MergeKeyInfo*, 16>> &left_merge_keys,
                          const ObIArray<ObRawExpr*> &left_join_keys,
                          const ObIArray<ObRawExpr*> &right_join_keys,
                          const ObIArray<bool> &null_safe_info,
                          const common::ObIArray<ObRawExpr*> &equal_join_conditions,
                          const common::ObIArray<ObRawExpr*> &other_join_conditions,
                          const common::ObIArray<ObRawExpr*> &filters,
                          const double equal_cond_sel,
                          const double other_cond_sel,
                          const ValidPathInfo &path_info);

    int generate_mj_paths(const EqualSets &equal_sets,
                          const ObIArray<Path*> &left_paths,
                          const ObIArray<Path*> &right_paths,
                          const ObIArray<MergeKeyInfo*> &left_merge_keys,
                          const ObIArray<ObRawExpr*> &left_join_keys,
                          const ObIArray<ObRawExpr*> &right_join_keys,
                          const ObIArray<bool> &null_safe_info,
                          const common::ObIArray<ObRawExpr*> &equal_join_conditions,
                          const common::ObIArray<ObRawExpr*> &other_join_conditions,
                          const common::ObIArray<ObRawExpr*> &filters,
                          const double equal_cond_sel,
                          const double other_cond_sel,
                          const ValidPathInfo &path_info);

    int find_minimal_cost_merge_path(const Path &left_path,
                                     const MergeKeyInfo &left_merge_key,
                                     const ObIArray<ObRawExpr*> &right_join_exprs,
                                     const ObIArray<Path*> &right_path_list,
                                     const DistAlgo join_dist_algo,
                                     const bool is_slave_mapping,
                                     ObIArray<OrderItem> &best_order_items,
                                     Path *&best_path,
                                     bool &best_need_sort,
                                     int64_t &best_prefix_pos,
                                     bool prune_mj);

    int init_merge_join_structure(ObIAllocator &allocator,
                                  const ObIArray<ObSEArray<Path*, 16>> &paths,
                                  const ObIArray<ObRawExpr*> &join_exprs,
                                  ObIArray<ObSEArray<MergeKeyInfo*, 16>> &merge_keys,
                                  const bool can_ignore_merge_plan);

    int init_merge_join_structure(common::ObIAllocator &allocator,
                                  const common::ObIArray<Path*> &paths,
                                  const common::ObIArray<ObRawExpr*> &join_exprs,
                                  common::ObIArray<MergeKeyInfo*> &merge_keys,
                                  const bool can_ignore_merge_plan);

    int push_down_order_siblings(JoinPath *join_path, const Path *right_path);

    int create_and_add_nl_path(const Path *left_path,
                               const Path *right_path,
                               const ObJoinType join_type,
                               const DistAlgo join_dist_algo,
                               const bool is_slave_mapping,
                               const common::ObIArray<ObRawExpr*> &on_conditions,
                               const common::ObIArray<ObRawExpr*> &where_conditions,
                               const bool has_equal_cond,
                               const bool is_normal_nl,
                               bool need_mat = false);

    int create_and_add_hash_path(const Path *left_path,
                                 const Path *right_path,
                                 const ObJoinType join_type,
                                 const DistAlgo join_dist_algo,
                                 const bool is_slave_mapping,
                                 const common::ObIArray<ObRawExpr*> &equal_join_conditions,
                                 const common::ObIArray<ObRawExpr*> &other_join_conditions,
                                 const common::ObIArray<ObRawExpr*> &filters,
                                 const double equal_cond_sel,
                                 const double other_cond_sel,
                                 const NullAwareAntiJoinInfo &naaj_info);

    int generate_join_filter_infos(const Path &left_path,
                                  const Path &right_path,
                                  const ObJoinType join_type,
                                  const DistAlgo join_dist_algo,
                                  const ObIArray<ObRawExpr*> &equal_join_conditions,
                                  const bool is_left_naaj_na,
                                  ObIArray<JoinFilterInfo> &join_filter_infos);

    int find_possible_join_filter_tables(const Path &left_path,
                                        const Path &right_path,
                                        const DistAlgo join_dist_algo,
                                        const ObIArray<ObRawExpr*> &equal_join_conditions,
                                        ObIArray<JoinFilterInfo> &join_filter_infos);

    int find_possible_join_filter_tables(const ObLogPlanHint &log_plan_hint,
                                        const Path &right_path,
                                        const ObRelIds &left_tables,
                                        const ObRelIds &right_tables,
                                        bool config_disable,
                                        bool is_current_dfo,
                                        bool is_fully_partition_wise,
                                        int64_t current_dfo_level,
                                        const ObIArray<ObRawExpr*> &left_join_conditions,
                                        const ObIArray<ObRawExpr*> &right_join_conditions,
                                        ObIArray<JoinFilterInfo> &join_filter_infos);

    int get_join_filter_exprs(const ObIArray<ObRawExpr*> &left_join_conditions,
                              const ObIArray<ObRawExpr*> &right_join_conditions,
                              JoinFilterInfo &join_filter_info);

    int fill_join_filter_info(JoinFilterInfo &join_filter_info);

    int check_normal_join_filter_valid(const Path& left_path,
                                       const Path& right_path,
                                       ObIArray<JoinFilterInfo> &join_filter_infos);

    int calc_join_filter_selectivity(const Path& left_path,
                                    JoinFilterInfo& info,
                                    double &join_filter_selectivity);

    int find_shuffle_join_filter(const Path& path, bool &find);

    int check_partition_join_filter_valid(const DistAlgo join_dist_algo,
                                          ObIArray<JoinFilterInfo> &join_filter_infos);

    int build_join_filter_part_expr(const int64_t ref_table_id,
                                    const common::ObIArray<ObRawExpr *> &lexprs,
                                    const common::ObIArray<ObRawExpr *> &rexprs,
                                    ObShardingInfo *sharding_info,
                                    ObRawExpr *&left_calc_part_id_expr,
                                    bool skip_subpart);

    int remove_invalid_join_filter_infos(ObIArray<JoinFilterInfo> &join_filter_infos);

    int create_and_add_mj_path(const Path *left_path,
                               const Path *right_path,
                               const ObJoinType join_type,
                               const DistAlgo join_dist_algo,
                               const bool is_slave_mapping,
                               const common::ObIArray<ObOrderDirection> &merge_directions,
                               const common::ObIArray<ObRawExpr*> &equal_join_conditions,
                               const common::ObIArray<ObRawExpr*> &other_join_conditions,
                               const common::ObIArray<ObRawExpr*> &filters,
                               const double equal_cond_sel,
                               const double other_cond_sel,
                               const common::ObIArray<OrderItem> &left_sort_keys,
                               const bool left_need_sort,
                               const int64_t left_prefix_pos,
                               const common::ObIArray<OrderItem> &right_sort_keys,
                               const bool right_need_sort,
                               const int64_t right_prefix_pos);

    int get_distributed_join_method(Path &left_path,
                                    Path &right_path,
                                    const EqualSets &equal_sets,
                                    const ObIArray<ObRawExpr*> &left_join_keys,
                                    const ObIArray<ObRawExpr*> &right_join_keys,
                                    const ObIArray<bool> &null_safe_info,
                                    const ValidPathInfo &path_info,
                                    const JoinAlgo join_algo,
                                    const bool is_push_down,
                                    const bool is_naaj,
                                    int64_t &distributed_types);

    bool is_partition_wise_valid(const Path &left_path,
                                 const Path &right_path);

    bool is_repart_valid(const Path &left_path, const Path &right_path, const DistAlgo dist_algo, const bool is_nl);

    int check_if_match_partition_wise(const EqualSets &equal_sets,
                                      Path &left_path,
                                      Path &right_path,
                                      const ObIArray<ObRawExpr*> &left_join_keys,
                                      const ObIArray<ObRawExpr*> &right_join_keys,
                                      const ObIArray<bool> &null_safe_info,
                                      bool &is_partition_wise);

    int extract_hashjoin_conditions(const ObIArray<ObRawExpr*> &join_quals,
                                    const ObRelIds &left_tables,
                                    const ObRelIds &right_tables,
                                    ObIArray<ObRawExpr*> &equal_join_conditions,
                                    ObIArray<ObRawExpr*> &other_join_conditions,
                                    const ObJoinType &join_type,
                                    NullAwareAntiJoinInfo &naaj_info);

    int check_is_join_equal_conditions(const ObRawExpr *equal_cond,
                                       const ObRelIds &left_tables,
                                       const ObRelIds &right_tables,
                                       bool &is_equal_cond);

    int classify_hashjoin_conditions(const ObJoinOrder &left_tree,
                                     const ObJoinOrder &right_tree,
                                     const ObJoinType join_type,
                                     const common::ObIArray<ObRawExpr*> &on_conditions,
                                     const common::ObIArray<ObRawExpr*> &where_conditionss,
                                     common::ObIArray<ObRawExpr*> &equal_join_conditions,
                                     common::ObIArray<ObRawExpr*> &other_join_conditions,
                                     common::ObIArray<ObRawExpr*> &filters,
                                     NullAwareAntiJoinInfo &naaj_info);

    int classify_mergejoin_conditions(const ObJoinOrder &left_tree,
                                      const ObJoinOrder &right_tree,
                                      const ObJoinType join_type,
                                      const common::ObIArray<ObRawExpr*> &on_conditions,
                                      const common::ObIArray<ObRawExpr*> &where_conditions,
                                      common::ObIArray<ObRawExpr*> &equal_join_conditions,
                                      common::ObIArray<ObRawExpr*> &other_join_condition,
                                      common::ObIArray<ObRawExpr*> &filters);
    /**
     * 从join_quals里解析出equal_join_conditions和other_join_conditions。
     * 传入的join_quals肯定是满足能够让left和right进行join的条件，完全不满足join的条件是不会传入的。
     * 例如：left=A，right=B，a=c是不可能在这里传入的。
     * 但是这里可能会传入a+b=c，这个条件能够使得A和B做Join，但是这个条件却不能放在这一层，必须等到条件中涉及的表都join完之后
     * 才能运算这个条件。
     * 提取策略是：join_quals是and连接的表达式，逐个检查join_qual_。
     * 如果是var_left=var_right， 或者var_right=var_left类型的表达式，则提取出来作为merge join condition，
     * 其他的类型分别作为merge join other condition。
     * 特别的，如果一个单表条件放在了join quals里，也认为可以作为merge join other condition。
     * 如果需要更复杂的提取策略，如Expr(var_left)=Expr(var_right)，今后再添加。
     * @param join_quals
     * @param equal_join_conditions
     * @param other_join_conditions
     * @return
     */
    int extract_mergejoin_conditions(const common::ObIArray<ObRawExpr*> &join_quals,
                                     const ObRelIds &left_tables,
                                     const ObRelIds &right_tables,
                                     common::ObIArray<ObRawExpr*> &equal_join_conditions,
                                     common::ObIArray<ObRawExpr*> &other_join_conditions);

    int extract_params_for_inner_path(const ObRelIds &join_relids,
                                      ObIArray<ObExecParamRawExpr *> &nl_params,
                                      ObIArray<ObRawExpr*> &subquery_exprs,
                                      const ObIArray<ObRawExpr*> &exprs,
                                      ObIArray<ObRawExpr*> &new_exprs);

    int is_onetime_expr(const ObRelIds &ignore_relids, ObRawExpr* expr, bool &is_valid);

    int create_onetime_expr(const ObRelIds &ignore_relids, ObRawExpr* &expr);

    int check_join_interesting_order(Path* path);

    int64_t get_diverse_path_count() const { return diverse_path_count_; }

    inline double get_anti_or_semi_match_sel() const { return anti_or_semi_match_sel_; }

    const ObFdItemSet &get_fd_item_set() const { return fd_item_set_; }

    ObFdItemSet &get_fd_item_set() { return fd_item_set_; }

    const ObTableMetaInfo &get_table_meta() const { return table_meta_info_; }

    InnerPathInfos &get_inner_path_infos() { return inner_path_infos_; }
    const InnerPathInfos &get_inner_path_infos() const { return inner_path_infos_; }

    int64_t get_name(char *buf, const int64_t buf_len)
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

    TO_STRING_KV(K_(type),
                 K_(output_rows),
                 K_(interesting_paths));
  private:
    int add_access_filters(AccessPath *path,
                           const common::ObIArray<ObRawExpr*> &index_keys,
                           const common::ObIArray<ObRawExpr*> &range_exprs,
                           PathHelper &helper);

    int set_nl_filters(JoinPath *join_path,
                       const Path *right_path,
                       const ObJoinType join_type,
                       const common::ObIArray<ObRawExpr*> &on_conditions,
                       const common::ObIArray<ObRawExpr*> &where_conditions);

    int fill_query_range_info(const QueryRangeInfo &range_info,
                              ObCostTableScanInfo &est_cost_info,
                              bool use_skip_scan);

    int set_table_location_for_paths(ObIArray<AccessPath *> &access_paths,
                                     ObIndexInfoCache &index_info_cache);

    int compute_table_location(const uint64_t table_id,
                               const uint64_t ref_id,
                               const bool is_global_index,
                               ObTablePartitionInfo *&table_partition_info);

    int get_query_range_info(const uint64_t table_id,
                             const uint64_t base_table_id,
                             const uint64_t index_id,
                             QueryRangeInfo &range_info,
                             PathHelper &helper);

    int check_has_exec_param(const ObQueryRange &query_range,
                             bool &has_exec_param);

    int get_preliminary_prefix_info(ObQueryRange &query_range,QueryRangeInfo &range_info);

    void get_prefix_info(const ObKeyPart *key_part,
                         int64_t &equal_prefix_count,
                         int64_t &range_prefix_count,
                         bool &contain_always_false);

    // @brief  check if an index is relevant to the conditions
    int is_relevant_index(const uint64_t table_id,
                          const uint64_t index_ref_id,
                          bool &relevant);

    int get_valid_index_ids(const uint64_t table_id,
                            const uint64_t ref_table_id,
                            ObIArray<uint64_t> &valid_index_id);
    int get_valid_index_ids_with_no_index_hint(ObSqlSchemaGuard &schema_guard,
                                               const uint64_t ref_table_id,
                                               uint64_t *tids,
                                               const int64_t index_count,
                                               const ObIArray<uint64_t> &ignore_index_ids,
                                               ObIArray<uint64_t> &valid_index_ids);
    // table heuristics
    int add_table_by_heuristics(const uint64_t table_id,
                                const uint64_t ref_table_id,
                                const ObIndexInfoCache &index_info_cache,
                                const ObIArray<uint64_t> &candi_index_ids,
                                ObIArray<uint64_t> &valid_index_ids,
                                PathHelper &helper);

    // table heuristics for a virtual table.
    int virtual_table_heuristics(const uint64_t table_id,
                                 const uint64_t ref_table_id,
                                 const ObIndexInfoCache &index_info_cache,
                                 const ObIArray<uint64_t> &valid_index_ids,
                                 uint64_t &index_to_use);

    // table heuristics for non-virtual table
    int user_table_heuristics(const uint64_t table_id,
                              const uint64_t ref_table_id,
                              const ObIndexInfoCache &index_info_cache,
                              const ObIArray<uint64_t> &valid_index_ids,
                              uint64_t &index_to_use,
                              PathHelper &helper);

    int refine_table_heuristics_result(const uint64_t table_id,
                                       const uint64_t ref_table_id,
                                       const common::ObIArray<uint64_t> &candidate_refine_idx,
                                       const common::ObIArray<uint64_t> &match_unique_idx,
                                       const ObIndexInfoCache &index_info_cache,
                                       uint64_t &index_to_use);

    int check_index_subset(const OrderingInfo *first_ordering_info,
                           const int64_t first_index_key_count,
                           const OrderingInfo *second_ordering_info,
                           const int64_t second_index_key_count,
                           DominateRelation &status);

    bool join_hint_match_tables(const ObIArray<LogJoinHint> &log_join_hints,
                                const ObRelIds& table_ids);
    /**
     * @brief Get path types needed to genereate
     * @need_path_types the types needed to genereate.
     */
    int get_valid_path_info(const ObJoinOrder &left_tree,
                            const ObJoinOrder &right_tree,
                            const ObJoinType join_type,
                            const ObIArray<ObRawExpr*> &join_conditions,
                            const bool ignore_hint,
                            const bool reverse_join_tree,
                            ValidPathInfo &path_info);
    int get_valid_path_info_from_hint(const ObRelIds &table_set,
                                      bool both_access,
                                      bool contain_fake_cte,
                                      ValidPathInfo &path_info);

    int check_depend_table(const ObJoinOrder &left_tree,
                           const ObJoinOrder &right_tree,
                           const ObJoinType join_type,
                           ValidPathInfo &path_info);
    int check_subquery_in_join_condition(const ObJoinType join_type,
                                         const ObIArray<ObRawExpr*> &join_conditions,
                                         ValidPathInfo &path_info);

    int make_mj_path(Path *left_path,
                     Path *right_path,
                     ObJoinType join_type,
                     common::ObIArray<ObRawExpr*> &join_conditions,
                     common::ObIArray<ObRawExpr*> &no_sort_conditions,
                     common::ObIArray<ObRawExpr*> &join_filters,
                     common::ObIArray<ObRawExpr*> &join_quals, Path** join_path);

    //find minimal cost path
    int find_minimal_cost_path(const common::ObIArray<Path *> &all_paths,
                               Path *&minimal_cost_path);

    int find_minimal_cost_path(const ObIArray<ObSEArray<Path*, 16>> &path_list,
                               ObIArray<Path*> &best_paths);

    int deduce_const_exprs_and_ft_item_set();

    int compute_fd_item_set_for_table_scan(const uint64_t table_id,
                                           const uint64_t table_ref_id,
                                           const ObIArray<ObRawExpr *> &quals);

    int compute_fd_item_set_for_join(const ObJoinOrder *left_tree,
                                     const ObJoinOrder *right_tree,
                                     const JoinInfo *join_info,
                                     const ObJoinType join_type);

    int compute_fd_item_set_for_inner_join(const ObJoinOrder *left_tree,
                                           const ObJoinOrder *right_tree,
                                           const JoinInfo *join_info);

    int compute_fd_item_set_for_semi_anti_join(const ObJoinOrder *left_tree,
                                               const ObJoinOrder *right_tree,
                                               const JoinInfo *join_info,
                                               const ObJoinType join_type);

    int compute_fd_item_set_for_outer_join(const ObJoinOrder *left_tree,
                                           const ObJoinOrder *right_tree,
                                           const JoinInfo *ljoin_info,
                                           const ObJoinType join_type);

    int compute_fd_item_set_for_subquery(const uint64_t table_id,
                                         ObLogicalOperator *subplan_root);

    int compute_one_row_info_for_join(const ObJoinOrder *left_tree,
                                      const ObJoinOrder *right_tree,
                                      const ObIArray<ObRawExpr*> &join_condition,
                                      const ObIArray<ObRawExpr*> &equal_join_condition,
                                      const ObJoinType join_type);

  private:
    int find_matching_cond(const ObIArray<ObRawExpr *> &join_conditions,
                           const OrderItem &left_ordering,
                           const OrderItem &right_ordering,
                           const EqualSets &equal_sets,
                           int64_t &common_prefix_idx);

  private:
    int compute_cost_and_prune_access_path(PathHelper &helper,
                                           ObIArray<AccessPath *> &access_paths);
    int revise_output_rows_after_creating_path(PathHelper &helper,
                                               ObIArray<AccessPath *> &access_paths);
    int fill_filters(const common::ObIArray<ObRawExpr *> &all_filters,
                     const ObQueryRange* query_range,
                     ObCostTableScanInfo &est_scan_cost_info,
                     bool &is_nl_with_extended_range,
                     bool is_link = false,
                     bool use_skip_scan = false);

    int can_extract_unprecise_range(const uint64_t table_id,
                                    const ObRawExpr *filter,
                                    const ObBitSet<> &ex_prefix_column_bs,
                                    bool &can_extract);

    int estimate_rowcount_for_access_path(ObIArray<AccessPath*> &all_paths,
                                          const bool is_inner_path,
                                          common::ObIArray<ObRawExpr*> &filter_exprs,
                                          ObBaseTableEstMethod &method);

    inline bool can_use_remote_estimate(OptimizationMethod method)
    {
      return OptimizationMethod::RULE_BASED != method;
    }

    int compute_table_rowcount_info();

    int increase_diverse_path_count(AccessPath *ap);

  public:
    inline double get_output_row_size() { return output_row_size_; }
    inline double get_output_row_size() const { return output_row_size_; }
    inline void set_output_row_size(const double output_row_size) { output_row_size_ = output_row_size; }
    static int calc_join_output_rows(ObLogPlan *plan,
                                     const ObRelIds &left_ids,
                                     const ObRelIds &right_ids,
                                     double left_output_rows,
                                     double right_output_rows,
                                     const JoinInfo &join_info,
                                     double &new_rows,
                                     double &selectivity,
                                     EqualSets &equal_sets);
    inline void set_cnt_rownum(const bool cnt_rownum) { cnt_rownum_ = cnt_rownum; }
    inline bool get_cnt_rownum() const { return cnt_rownum_; }
    inline void increase_total_path_num() { total_path_num_ ++; }
    inline uint64_t get_total_path_num() const { return total_path_num_; }
    int get_join_output_exprs(ObIArray<ObRawExpr *> &output_exprs);
    int get_excluded_condition_exprs(ObIArray<ObRawExpr *> &excluded_conditions);
    static double calc_single_parallel_rows(double rows, int64_t parallel);
  private:
    static int check_and_remove_is_null_qual(ObLogPlan *plan,
                                             const ObJoinType join_type,
                                             const ObRelIds &left_ids,
                                             const ObRelIds &right_ids,
                                             const ObIArray<ObRawExpr*> &quals,
                                             ObIArray<ObRawExpr*> &normal_quals,
                                             bool &left_has_is_null,
                                             bool &right_has_is_null);

    int get_cached_inner_paths(const ObIArray<ObRawExpr *> &join_conditions,
                               ObJoinOrder &left_tree,
                               ObJoinOrder &right_tree,
                               const bool force_inner_nl,
                               ObIArray<Path *> &inner_paths);

    int generate_inner_base_paths(const ObIArray<ObRawExpr *> &join_conditions,
                                  ObJoinOrder &left_tree,
                                  ObJoinOrder &right_tree,
                                  InnerPathInfo &inner_path_info);

    int generate_inner_base_table_paths(const ObIArray<ObRawExpr *> &join_conditions,
                                        ObJoinOrder &left_tree,
                                        ObJoinOrder &right_tree,
                                        InnerPathInfo &inner_path_info);

    int check_inner_path_valid(const ObIArray<ObRawExpr *> &join_conditions,
                               const ObRelIds &join_relids,
                               ObJoinOrder &right_tree,
                               const bool force_inner_nl,
                               ObIArray<ObRawExpr *> &pushdown_quals,
                               ObIArray<ObRawExpr *> &param_pushdown_quals,
                               ObIArray<ObExecParamRawExpr *> &nl_params,
                               ObIArray<ObRawExpr *> &subquery_exprs,
                               bool &is_valid);

    int remove_redudant_filter(ObJoinOrder &left_tree,
                              ObJoinOrder &right_tree,
                              const ObIArray<ObRawExpr *> &join_conditions,
                              ObIArray<ObRawExpr *> &filters,
                              ObIArray<ObPCParamEqualInfo> &equal_param_constraints);

    int check_filter_is_redundant(ObJoinOrder &left_tree,
                                  ObRawExpr *expr,
                                  ObExprParamCheckContext &context,
                                  bool &is_redunant);

    int generate_inner_subquery_paths(const ObIArray<ObRawExpr *> &join_conditions,
                                      const ObRelIds &join_relids,
                                      ObJoinOrder &right_tree,
                                      InnerPathInfo &inner_path_info);

    int generate_inner_subquery_paths(const ObDMLStmt &parent_stmt,
                                      const ObRelIds join_relids,
                                      const ObIArray<ObRawExpr*> &pushdown_quals,
                                      InnerPathInfo &inner_path_info);


    int generate_force_inner_path(const ObIArray<ObRawExpr *> &join_conditions,
                                  const ObRelIds join_relids,
                                  ObJoinOrder &right_tree,
                                  InnerPathInfo &inner_path_info);

    int copy_path(const Path& src_path, Path* &dst_path);

    int check_and_fill_inner_path_info(PathHelper &helper,
                                       const ObDMLStmt &stmt,
                                       const EqualSets &equal_sets,
                                       InnerPathInfo &inner_path_info,
                                       const ObIArray<ObRawExpr *> &pushdown_quals,
                                       ObIArray<ObExecParamRawExpr *> &nl_params);

    int extract_pushdown_quals(const ObIArray<ObRawExpr *> &quals,
                               const bool force_inner_nl,
                               ObIArray<ObRawExpr *> &pushdown_quals);

    int get_generated_col_index_qual(const int64_t table_id,
                                     ObIArray<ObRawExpr *> &quals,
                                     PathHelper &helper);

    int build_prefix_index_compare_expr(ObRawExpr &column_expr,
                                        ObRawExpr *prefix_expr,
                                        ObItemType type,
                                        ObRawExpr &value_expr,
                                        ObRawExpr *escape_expr,
                                        ObRawExpr *&new_op_expr,
                                        PathHelper &helper);

    int get_prefix_str_idx_exprs(ObRawExpr *expr,
                                ObColumnRefRawExpr *column_expr,
                                ObRawExpr *value_expr,
                                ObRawExpr *escape_expr,
                                const TableItem *table_item,
                                ObItemType type,
                                ObIArray<ObRawExpr*> &new_exprs,
                                PathHelper &helper);

    int deduce_prefix_str_idx_exprs(ObRawExpr *expr,
                                    const TableItem *table_item,
                                    ObIArray<ObRawExpr*> &new_exprs,
                                    PathHelper &helper);

    int deduce_common_gen_col_index_expr(ObRawExpr *qual,
                                        const TableItem *table_item,
                                        ObRawExpr *&new_qual);

    int try_get_generated_col_index_expr(ObRawExpr *qual,
                                         ObRawExpr *depend_expr,
                                         ObColumnRefRawExpr *col_expr,
                                         ObRawExpr *&new_qual);

    int try_get_json_generated_col_index_expr(ObRawExpr *depend_expr,
                                              ObColumnRefRawExpr *col_expr,
                                              ObRawExprCopier& copier,
                                              ObRawExprFactory& expr_factory,
                                              ObSQLSessionInfo *session_info,
                                              ObRawExpr *&qual,
                                              int64_t qual_pos,
                                              ObRawExpr *&new_qual);
    int get_range_params(const Path *path,
                         ObIArray<ObRawExpr*> &range_exprs,
                         ObIArray<ObRawExpr*> &all_table_filters);

    int find_best_inner_nl_path(const ObIArray<Path*> &inner_paths,
                                Path *&best_nl_path);

    InnerPathInfo* get_inner_path_info(const ObIArray<ObRawExpr *> &join_conditions);

    int extract_real_join_keys(ObIArray<ObRawExpr *> &join_keys);

    int extract_naaj_join_conditions(const ObIArray<ObRawExpr*> &join_quals,
                                     const ObRelIds &left_tables,
                                     const ObRelIds &right_tables,
                                     ObIArray<ObRawExpr*> &equal_join_conditions,
                                     NullAwareAntiJoinInfo &naaj_info);
    bool is_main_table_use_das(const common::ObIArray<AccessPath *> &access_paths);
    int add_deduced_expr(ObRawExpr *deduced_expr, ObRawExpr *deduce_from, bool is_persistent);
    int add_deduced_expr(ObRawExpr *deduced_expr, ObRawExpr *deduce_from,
                          bool is_persistent, ObExprEqualCheckContext &equal_ctx);
    int check_match_to_type(ObRawExpr *to_type, ObRawExpr *candi_expr, bool &is_same, ObExprEqualCheckContext &equal_ctx);
    int check_can_use_global_stat_instead(const uint64_t ref_table_id,
                                          const ObTableSchema &table_schema,
                                          ObIArray<int64_t> &all_used_parts,
                                          ObIArray<ObTabletID> &all_used_tablets,
                                          bool &can_use,
                                          ObIArray<int64_t> &global_part_ids,
                                          double &scale_ratio);
    int is_valid_range_expr_for_oracle_agent_table(const ObRawExpr *range_expr, bool &is_valid);
    int extract_valid_range_expr_for_oracle_agent_table(const common::ObIArray<ObRawExpr *> &filters,
                                                        common::ObIArray<ObRawExpr *> &new_filters);
    bool virtual_table_index_can_range_scan(uint64_t table_id);
    int compute_table_location_for_index_info_entry(const uint64_t table_id,
                                                    const uint64_t ref_table_id,
                                                    IndexInfoEntry *index_info_entry);
    int compute_sharding_info_for_index_info_entry(const uint64_t table_id,
                                                   const uint64_t base_table_id,
                                                   IndexInfoEntry *index_info_entry);
    friend class ::test::TestJoinOrder_ob_join_order_param_check_Test;
    friend class ::test::TestJoinOrder_ob_join_order_src_Test;
  private:
    common::ObIAllocator *allocator_;
    ObLogPlan *plan_;
    PathType type_;
    uint64_t table_id_; //如果是基表（Base table/Generated table/Joined table）记录table id
    ObRelIds table_set_; //存在这里的是TableItem所在的下标
    ObRelIds output_table_set_; //需要输出的table item下表, 经过semi join时只输出左枝
    double output_rows_;
    double output_row_size_;
    double anti_or_semi_match_sel_; //for anti/semi join
    ObTablePartitionInfo *table_partition_info_; // only for base table
    ObShardingInfo *sharding_info_; // only for base table and local index
    ObTableMetaInfo table_meta_info_; // only for base table
    JoinInfo* join_info_; //记录连接信息
    common::ObSEArray<ObConflictDetector*, 8, common::ModulePageAllocator, true> used_conflict_detectors_; //记录当前join order用掉了哪些冲突检测器
    common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> restrict_info_set_; //对于基表（SubQuery）记录单表条件；对于普通Join为空
    common::ObSEArray<Path*, 32, common::ModulePageAllocator, true> interesting_paths_;
    bool is_at_most_one_row_;
    EqualSets output_equal_sets_;
    common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> output_const_exprs_;
    BaseTableOptInfo table_opt_info_;
    common::ObSEArray<AccessPath*, 4, common::ModulePageAllocator, true> available_access_paths_;
    int64_t diverse_path_count_; // number of access path with diverse query range
    ObFdItemSet fd_item_set_;
    ObFdItemSet candi_fd_item_set_;
    ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> not_null_columns_;
    // cache for all inner path
    InnerPathInfos inner_path_infos_;
    common::ObSEArray<DeducedExprInfo, 4, common::ModulePageAllocator, true> deduced_exprs_info_;
    bool cnt_rownum_;
    uint64_t total_path_num_;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObJoinOrder);
  };
}
}

#endif
