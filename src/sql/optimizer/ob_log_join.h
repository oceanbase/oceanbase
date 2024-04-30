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

#ifndef OCEANBASE_SQL_OB_LOG_JOIN_H
#define OCEANBASE_SQL_OB_LOG_JOIN_H
#include "ob_log_operator_factory.h"
#include "ob_logical_operator.h"
#include "ob_join_order.h"
namespace oceanbase
{
namespace sql
{
  class ObLogicalOperator;
  class ObLogJoin : public ObLogicalOperator
  {
  public:
    ObLogJoin(ObLogPlan &plan)
      : ObLogicalOperator(plan),
        join_conditions_(),
        join_filters_(),
        join_type_(UNKNOWN_JOIN),
        join_algo_(INVALID_JOIN_ALGO),
        join_dist_algo_(DistAlgo::DIST_INVALID_METHOD),
        late_mat_(false),
        merge_directions_(),
        nl_params_(),
        connect_by_pseudo_columns_(),
        connect_by_prior_exprs_(),
        prior_exprs_(),
        connect_by_root_exprs_(),
        sys_connect_by_path_exprs_(),
        partition_id_expr_(nullptr),
        slave_mapping_type_(SM_NONE),
        connect_by_extra_exprs_(),
        enable_px_batch_rescan_(false),
        can_use_batch_nlj_(false),
        join_path_(nullptr)
    { }
    virtual ~ObLogJoin() {}

    inline void set_join_type(const ObJoinType join_type) { join_type_ = join_type; }
    inline ObJoinType get_join_type() const { return join_type_; }
    inline bool is_right_semi_or_anti_join() const { return join_type_ == RIGHT_SEMI_JOIN ||
                                                            join_type_ == RIGHT_ANTI_JOIN;}
    inline void set_join_algo(const JoinAlgo join_algo) { join_algo_ = join_algo; }
    inline JoinAlgo get_join_algo() const { return join_algo_; }
    inline void set_late_mat(const bool late_mat) { late_mat_ = late_mat; }
    inline bool is_late_mat() const { return late_mat_; }
    inline void set_join_distributed_method(const DistAlgo dist_method ) { join_dist_algo_ = dist_method; }
    inline DistAlgo get_join_distributed_method() const { return join_dist_algo_; }
    inline bool is_cartesian() const { return join_conditions_.empty() && join_filters_.empty() &&
                                              nl_params_.empty() && filter_exprs_.empty(); }
    inline DistAlgo get_dist_method() const { return join_dist_algo_; }
    inline bool is_shared_hash_join() const
    { return HASH_JOIN == join_algo_ && DIST_BC2HOST_NONE == join_dist_algo_; }
    int is_left_unique(bool &left_unique) const;
    inline int add_join_condition(ObRawExpr *expr) { return join_conditions_.push_back(expr); }
    inline int add_join_filter(ObRawExpr *expr) { return join_filters_.push_back(expr); }
    const common::ObIArray<ObRawExpr *> &get_equal_join_conditions() const { return join_conditions_; }
    const common::ObIArray<ObRawExpr *> &get_other_join_conditions() const { return join_filters_; }
    /**
     *  Get the nl params
     */
    inline common::ObIArray<ObExecParamRawExpr *> &get_nl_params() { return nl_params_; }
    /**
     *  Set the nl params
     */
    int set_nl_params(const common::ObIArray<ObExecParamRawExpr *> &params) { return append(nl_params_, params); }

    inline common::ObIArray<ObRawExpr*> &get_connect_by_pseudo_columns() { return connect_by_pseudo_columns_; }

    inline common::ObIArray<ObRawExpr *> &get_connect_by_prior_exprs() { return connect_by_prior_exprs_; }
    inline common::ObIArray<ObRawExpr *> &get_prior_exprs() { return prior_exprs_; }
    inline common::ObIArray<ObRawExpr *> &get_connect_by_root_exprs() { return connect_by_root_exprs_; }
    inline common::ObIArray<ObRawExpr *> &get_sys_connect_by_path_exprs() { return sys_connect_by_path_exprs_; }
    inline common::ObIArray<ObRawExpr *> &get_connect_by_extra_exprs() { return connect_by_extra_exprs_; }

    int set_connect_by_prior_exprs(const common::ObIArray<ObRawExpr *> &exprs) { return connect_by_prior_exprs_.assign(exprs); }

    inline ObLogicalOperator *get_left_table() const { return get_child(first_child); }
    inline ObLogicalOperator *get_right_table() const { return get_child(second_child); }

    //@brief Set all the join predicates
    int set_join_conditions(const common::ObIArray<ObRawExpr *> &conditions) { return append(join_conditions_, conditions); }

    int set_join_filters(const common::ObIArray<ObRawExpr *> &filters) { return append(join_filters_, filters); }

    common::ObIArray<ObRawExpr *> &get_join_conditions() { return join_conditions_; }

    const common::ObIArray<ObRawExpr *> &get_join_conditions() const { return join_conditions_; }

    common::ObIArray<ObRawExpr *> &get_join_filters() { return join_filters_; }
    int adjust_join_conds(ObIArray<ObRawExpr *> &dest_exprs);
    int calc_equal_cond_opposite(const ObRawExpr &raw_expr,
                                  bool &is_opposite);

    virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
    const common::ObIArray<ObOrderDirection> &get_merge_directions() const { return merge_directions_; }
    int set_merge_directions(const common::ObIArray<ObOrderDirection> &merge_directions)
    {
      return merge_directions_.assign(merge_directions);
    }

    /**
     *  Get the operator's hash value
     */
    virtual uint64_t hash(uint64_t seed) const override;

    virtual int get_explain_name_internal(char *buf,
                                          const int64_t buf_len,
                                          int64_t &pos);
    virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
    /*
     * IN         right_child_sharding_info   the join's right child sharding info
     * IN         right_keys                  the right join equal condition
     * OUT        type                        the type of bloom partition filter
     * */
    int bloom_filter_partition_type(const ObShardingInfo &right_child_sharding_info,
                                    ObIArray<ObRawExpr *> &right_keys,
                                    PartitionFilterType &type);
    virtual bool is_block_input(const int64_t child_idx) const override;
    virtual bool is_consume_child_1by1() const { return HASH_JOIN == join_algo_; }

    inline bool is_nlj_with_param_down() const { return (NESTED_LOOP_JOIN == join_algo_) &&
                                                        !nl_params_.empty(); }
    inline bool is_nlj_without_param_down() const { return (NESTED_LOOP_JOIN == join_algo_) &&
                                                            nl_params_.empty(); }
    virtual int compute_table_set() override;
    bool is_enable_gi_partition_pruning() const { return nullptr != partition_id_expr_; }
    ObOpPseudoColumnRawExpr *get_partition_id_expr() { return partition_id_expr_; }
    virtual int compute_property(Path *path) override;
    int set_connect_by_extra_exprs(const common::ObIArray<ObRawExpr *> &exprs)
    {
      return connect_by_extra_exprs_.assign(exprs);
    }
    void set_slave_mapping_type(SlaveMappingType slave_mapping_type)
    {
      slave_mapping_type_ = slave_mapping_type;
    }

    inline bool enable_px_batch_rescan() { return enable_px_batch_rescan_; }
    inline void set_px_batch_rescan(bool flag) { enable_px_batch_rescan_ = flag; }

    int set_join_filter_infos(const common::ObIArray<JoinFilterInfo> &infos) { return join_filter_infos_.assign(infos); }
    const common::ObIArray<JoinFilterInfo> &get_join_filter_infos() const { return join_filter_infos_; }

    inline bool can_use_batch_nlj() const { return can_use_batch_nlj_; }
    void set_can_use_batch_nlj(bool can_use) { can_use_batch_nlj_ = can_use; }
    int check_and_set_use_batch();
    int check_if_disable_batch(ObLogicalOperator* root, bool &can_use_batch_nlj);
    void set_join_path(JoinPath *path) { join_path_ = path; }
    JoinPath *get_join_path() { return join_path_; }
    bool is_my_exec_expr(const ObRawExpr *expr);
    virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
    common::ObIArray<ObExecParamRawExpr *> &get_above_pushdown_left_params() { return above_pushdown_left_params_; }
    common::ObIArray<ObExecParamRawExpr *> &get_above_pushdown_right_params() { return above_pushdown_right_params_; }
    virtual int get_card_without_filter(double &card) override;

  private:
    int set_use_batch(ObLogicalOperator* root);
    inline bool can_enable_gi_partition_pruning()
    {
      return (NESTED_LOOP_JOIN == join_algo_)
          && join_dist_algo_ == DistAlgo::DIST_PARTITION_NONE;
    }
    int build_gi_partition_pruning();
    int set_granule_repart_ref_table_id_recursively(ObLogicalOperator *op, int64_t ref_table_id);
    // 在 NLJ 上分配一个 partition id，作为 consumer
    // 左侧的 GI 看到这个 partition id 后会以 producer 的身份生成列
    int generate_join_partition_id_expr();
    virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
    int get_connect_by_exprs(ObIArray<ObRawExpr*> &all_exprs);
    virtual int allocate_granule_post(AllocGIContext &ctx) override;
    virtual int allocate_granule_pre(AllocGIContext &ctx) override;
    int get_pq_distribution_method(const DistAlgo join_dist_algo,
                                   ObPQDistributeMethod::Type &left_dist_method,
                                   ObPQDistributeMethod::Type &right_dist_method);
    bool is_using_slave_mapping() { return SM_NONE != slave_mapping_type_; }
    int allocate_startup_expr_post() override;
    int allocate_startup_expr_post(int64_t child_idx) override;

    // print outline
    virtual int print_outline_data(PlanText &plan_text) override;
    virtual int print_used_hint(PlanText &plan_text) override;
    int add_used_leading_hint(ObIArray<const ObHint*> &used_hints);
    int check_used_leading(const ObIArray<LeadingInfo> &leading_infos,
                           const ObLogicalOperator *op,
                           bool &used_hint);
    bool find_leading_info(const ObIArray<LeadingInfo> &leading_infos,
                           const ObRelIds &l_set,
                           const ObRelIds &r_set);
    const ObLogicalOperator *find_child_join(const ObLogicalOperator *input_op);
    bool is_scan_operator(log_op_def::ObLogOpType type);
    int append_used_join_hint(ObIArray<const ObHint*> &used_hints);
    int append_used_join_filter_hint(ObIArray<const ObHint*> &used_hints);
    int print_join_hint_outline(const ObDMLStmt &stmt,
                                const ObItemType hint_type,
                                const ObString &qb_name,
                                const ObRelIds &table_set,
                                PlanText &plan_text);
    int print_join_filter_hint_outline(const ObDMLStmt &stmt,
                                       const ObString &qb_name,
                                       const ObRelIds &left_table_set,
                                       const uint64_t filter_table_id,
                                       const ObTableInHint &child_table_hint,
                                       const uint64_t child_table_id,
                                       const bool is_part_hint,
                                       PlanText &plan_text);
    int print_leading_tables(const ObDMLStmt &stmt,
                             PlanText &plan_text,
                             const ObLogicalOperator *op);
    int print_join_tables_in_hint(const ObDMLStmt &stmt,
                                  PlanText &plan_text,
                                  const ObRelIds &table_set);
    virtual int check_use_child_ordering(bool &used, int64_t &inherit_child_ordering_index)override;
  private:
    // all join predicates
    common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> join_conditions_; //equal join condition, for merge-join
    common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> join_filters_; //join filter, all conditions are here for nested loop join.
    ObJoinType join_type_;
    JoinAlgo join_algo_;
    DistAlgo join_dist_algo_;
    bool late_mat_;
    common::ObSEArray<ObOrderDirection, 8, common::ModulePageAllocator, true> merge_directions_;
    common::ObSEArray<ObExecParamRawExpr *, 8, common::ModulePageAllocator, true> nl_params_;
    common::ObSEArray<ObRawExpr*, 3, common::ModulePageAllocator, true> connect_by_pseudo_columns_;
    common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> connect_by_prior_exprs_;
    common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> prior_exprs_;
    common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> connect_by_root_exprs_;
    common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> sys_connect_by_path_exprs_;
    // NLJ 模式下右侧接 GI 时，通知 GI 过滤掉不可能命中的分区，以提升 rescan 性能
    // NLJ 模式下记录左侧 pkey exchange 生成的 partition id 列
    // 用于在 CG 阶段定位 part id 列位置，然后生成行下标
    ObOpPseudoColumnRawExpr *partition_id_expr_;
    SlaveMappingType slave_mapping_type_;
    ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> connect_by_extra_exprs_;
    // for nestloop join
    bool enable_px_batch_rescan_;
    common::ObSEArray<JoinFilterInfo, 4, common::ModulePageAllocator, true> join_filter_infos_;
    bool can_use_batch_nlj_;
    JoinPath *join_path_;
    common::ObSEArray<ObExecParamRawExpr *, 4, common::ModulePageAllocator, true> above_pushdown_left_params_;
    common::ObSEArray<ObExecParamRawExpr *, 4, common::ModulePageAllocator, true> above_pushdown_right_params_;

    DISALLOW_COPY_AND_ASSIGN(ObLogJoin);
  };

} // end of namespace sql
} // end of namespace oceanbase

#endif // OCEANBASE_SQL_OB_LOG_JOIN_H
