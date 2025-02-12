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

#ifndef OCEANBASE_SQL_OB_LOG_GROUP_BY_H
#define OCEANBASE_SQL_OB_LOG_GROUP_BY_H
#include "lib/allocator/page_arena.h"
#include "ob_logical_operator.h"
#include "ob_select_log_plan.h"
#include "sql/engine/aggregate/ob_adaptive_bypass_ctrl.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
class ObLogSort;
struct ObThreeStageAggrInfo
{
  ObThreeStageAggrInfo() :
    aggr_stage_(ObThreeStageAggrStage::NONE_STAGE),
    distinct_aggr_count_(-1),
    aggr_code_idx_(-1),
    aggr_code_expr_(NULL),
    distinct_aggr_batch_(),
    distinct_exprs_(),
    aggr_code_ndv_(1.0)
  {}

  ObThreeStageAggrStage aggr_stage_;
  int64_t distinct_aggr_count_;
  int64_t aggr_code_idx_;
  ObRawExpr *aggr_code_expr_;
  ObArray<ObDistinctAggrBatch, ModulePageAllocator, true> distinct_aggr_batch_;
  common::ObArray<ObRawExpr *, common::ModulePageAllocator, true> distinct_exprs_;
  double aggr_code_ndv_;

  int assign(const ObThreeStageAggrInfo &info);

  void reuse() {
    aggr_stage_ = ObThreeStageAggrStage::NONE_STAGE;
    aggr_code_idx_ = -1;
    aggr_code_expr_ = NULL;
    distinct_aggr_batch_.reuse();
    distinct_exprs_.reuse();
    aggr_code_ndv_ = 1.0;
  }

  int set_first_stage_info(ObRawExpr *aggr_code_expr, ObIArray<ObDistinctAggrBatch> &batch, double aggr_code_ndv);
  int set_second_stage_info(ObRawExpr *aggr_code_expr, ObIArray<ObDistinctAggrBatch> &batch, ObIArray<ObRawExpr *> &distinct_exprs);
  int set_third_stage_info(ObRawExpr *aggr_code_expr, ObIArray<ObDistinctAggrBatch> &batch);
};

struct ObRollupAdaptiveInfo
{
  ObRollupAdaptiveInfo()
  : rollup_id_expr_(NULL),
    rollup_status_(ObRollupStatus::NONE_ROLLUP),
    sort_keys_(),
    ecd_sort_keys_(),
    enable_encode_sort_(false)
  {}

  ObRawExpr *rollup_id_expr_;
  ObRollupStatus rollup_status_;
  ObArray<OrderItem, common::ModulePageAllocator, true> sort_keys_;
  ObArray<OrderItem, common::ModulePageAllocator, true> ecd_sort_keys_;
  bool enable_encode_sort_;

  int assign(const ObRollupAdaptiveInfo &info);
};

struct ObHashRollupInfo
{
  ObIArray<ObRawExpr *> *expand_exprs_;
  ObIArray<ObRawExpr *> *gby_exprs_;
  ObIArray<ObTuple<ObRawExpr *, ObRawExpr *>> *dup_expr_pairs_;
  ObRawExpr *rollup_grouping_id_;

  int assign(const ObHashRollupInfo &info);
  bool valid() const { return rollup_grouping_id_ != nullptr; }
};

class ObLogGroupBy : public ObLogicalOperator
{
public:
  ObLogGroupBy(ObLogPlan &plan)
      : ObLogicalOperator(plan),
        group_exprs_(),
        rollup_exprs_(),
        aggr_exprs_(),
        algo_(AGGREGATE_UNINITIALIZED),
        from_pivot_(false),
        is_push_down_(false),
        is_partition_gi_(false),
        total_ndv_(-1.0),
        origin_child_card_(-1.0),
        three_stage_info_(),
        rollup_adaptive_info_(),
        force_push_down_(false),
        use_hash_aggr_(false),
        has_push_down_(false),
        use_part_sort_(false),
        dist_method_(T_INVALID),
        gby_dop_(ObGlobalHint::UNSET_PARALLEL),
        is_pushdown_scalar_aggr_(false),
        hash_rollup_info_()
  {}
  virtual ~ObLogGroupBy()
  {}

  //const char* get_name() const;
  virtual int get_explain_name_internal(char *buf,
                                        const int64_t buf_len,
                                        int64_t &pos);
  int set_three_stage_info(const ObThreeStageAggrInfo &info);

  int set_rollup_info(const ObRollupStatus rollup_status,
                               ObRawExpr *rollup_id_expr);
  int set_rollup_info(const ObRollupStatus rollup_status,
                      ObRawExpr *rollup_id_expr,
                      ObIArray<OrderItem> &sort_keys,
                      ObIArray<OrderItem> &ecd_sort_keys,
                      bool enable_encode_sort);

  const ObIArray<ObDistinctAggrBatch> &get_distinct_aggr_batch()
  { return three_stage_info_.distinct_aggr_batch_; }


  // Get the 'group-by' expressions
  inline common::ObIArray<ObRawExpr *> &get_group_by_exprs()
  { return group_exprs_; }
  // Get the 'rollup' expressions
  inline common::ObIArray<ObRawExpr *> &get_rollup_exprs()
  { return rollup_exprs_; }
  // Get the aggregate expressions
  inline common::ObIArray<ObRawExpr *> &get_aggr_funcs()
  { return aggr_exprs_; }
  inline bool has_rollup()
  { return rollup_exprs_.count() > 0; }
  inline void set_hash_type() { algo_ = HASH_AGGREGATE; }
  inline void set_merge_type() { algo_ = MERGE_AGGREGATE; }
  inline void set_scalar_type() { algo_ = SCALAR_AGGREGATE; }
  inline void set_algo_type(AggregateAlgo type) { algo_ = type; }
  inline AggregateAlgo get_algo() const { return algo_; }

  // @brief SET the GROUP-BY COLUMNS
  int set_group_by_exprs(const common::ObIArray<ObRawExpr *> &group_by_exprs);
  // @brief SET the ROLLUP COLUMNS
  int set_rollup_exprs(const common::ObIArray<ObRawExpr *> &rollup_exprs);
  int set_aggr_exprs(const common::ObIArray<ObAggFunRawExpr *> &aggr_exprs);
  ObSelectLogPlan *get_plan() { return static_cast<ObSelectLogPlan *>(my_plan_); }
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
  virtual uint64_t hash(uint64_t seed) const override;
  virtual int est_cost() override;
  virtual int est_width() override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
  int inner_est_cost(const int64_t parallel,
                     double child_card,
                     double &child_ndv,
                     double &op_cost);
  int get_child_est_info(const int64_t parallel, double &child_card, double &child_ndv, double &selectivity);
  int get_gby_output_exprs(ObIArray<ObRawExpr *> &output_exprs);
  virtual bool is_block_op() const override
  { return (MERGE_AGGREGATE != get_algo() && !is_adaptive_aggregate())
        || ObRollupStatus::ROLLUP_DISTRIBUTOR == rollup_adaptive_info_.rollup_status_; }

  virtual int compute_const_exprs() override;
  virtual int compute_equal_set() override;
  virtual int compute_fd_item_set() override;
  virtual int compute_op_ordering() override;
  bool from_pivot() const { return from_pivot_; }
  void set_from_pivot(const bool value) { from_pivot_ = value; }
  int get_group_rollup_exprs(common::ObIArray<ObRawExpr *> &group_rollup_exprs) const;
  inline bool is_push_down() const { return is_push_down_; }
  inline void set_push_down(const bool is_push_down) { is_push_down_ = is_push_down; }
  inline void set_partition_gi(bool is_partition_gi) { is_partition_gi_ = is_partition_gi; }
  inline bool is_partition_gi() { return is_partition_gi_; }
  inline double get_total_ndv() const { return total_ndv_; }
  inline void set_total_ndv(double total_ndv) { total_ndv_ = total_ndv; }
  inline double get_origin_child_card() const { return origin_child_card_; }
  inline void set_origin_child_card(double card) { origin_child_card_ = card; }
  inline bool force_partition_gi() const { return (is_partition_wise() && !is_push_down()) || (is_partition_gi_); }


  int allocate_startup_expr_post()override;

  inline ObThreeStageAggrStage get_aggr_stage() const { return three_stage_info_.aggr_stage_; }
  inline int64_t get_aggr_code_idx() const { return three_stage_info_.aggr_code_idx_; }
  inline ObRawExpr* get_aggr_code_expr() { return three_stage_info_.aggr_code_expr_; }
  inline int64_t get_distinct_aggr_count() const { return three_stage_info_.distinct_aggr_count_; }
  inline common::ObIArray<ObRawExpr *> &get_distinct_exprs() { return three_stage_info_.distinct_exprs_; }

  inline bool is_three_stage_aggr() const { return ObThreeStageAggrStage::NONE_STAGE != three_stage_info_.aggr_stage_; }
  inline bool is_first_stage() const { return ObThreeStageAggrStage::FIRST_STAGE == three_stage_info_.aggr_stage_; }
  inline bool is_second_stage() const { return ObThreeStageAggrStage::SECOND_STAGE == three_stage_info_.aggr_stage_; }
  inline bool is_third_stage() const { return ObThreeStageAggrStage::THIRD_STAGE == three_stage_info_.aggr_stage_; }
  inline bool force_push_down() const { return force_push_down_; }
  inline bool is_adaptive_aggregate() const { return HASH_AGGREGATE == get_algo()
                                                     && !force_push_down()
                                                     && (is_first_stage() || (!is_three_stage_aggr() && is_push_down())); }


  inline void set_rollup_status(const ObRollupStatus rollup_status)
  { rollup_adaptive_info_.rollup_status_ = rollup_status; }
  inline ObRollupStatus get_rollup_status() const
  { return rollup_adaptive_info_.rollup_status_; }
  inline ObRawExpr *get_rollup_id_expr()
  { return rollup_adaptive_info_.rollup_id_expr_; }
  inline ObIArray<OrderItem> &get_inner_sort_keys()
  { return rollup_adaptive_info_.sort_keys_; }
  inline ObIArray<OrderItem> &get_inner_ecd_sort_keys()
  { return rollup_adaptive_info_.ecd_sort_keys_; }
  inline bool has_encode_sort()
  { return rollup_adaptive_info_.enable_encode_sort_; }
  inline bool is_rollup_distributor() const
  { return ObRollupStatus::ROLLUP_DISTRIBUTOR == rollup_adaptive_info_.rollup_status_; }
  inline bool is_rollup_collector() const
  { return ObRollupStatus::ROLLUP_COLLECTOR == rollup_adaptive_info_.rollup_status_; }
  inline void set_force_push_down(bool force_push_down)
  { force_push_down_ = force_push_down; }
  void set_group_by_outline_info(DistAlgo algo,
                                 bool use_hash_aggr,
                                 bool has_push_down,
                                 bool use_part_sort = false,
                                 int64_t dop = ObGlobalHint::UNSET_PARALLEL)
  {
    dist_method_ = DistAlgo::DIST_BASIC_METHOD == algo ? T_DISTRIBUTE_BASIC :
                  (DistAlgo::DIST_PARTITION_WISE == algo ? T_DISTRIBUTE_NONE :
                  (DistAlgo::DIST_HASH_HASH == algo ? T_DISTRIBUTE_HASH : T_DISTRIBUTE_LOCAL));

    use_hash_aggr_ = use_hash_aggr;
    has_push_down_ = has_push_down;
    use_part_sort_ = use_part_sort;
    gby_dop_ = dop;
  }
  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;

  virtual int compute_sharding_info() override;

  // used for the rowcount estimation of the first stage
  double get_number_of_copies() { return is_first_stage() ? three_stage_info_.aggr_code_ndv_ : 1.0; };

  void set_pushdown_scalar_aggr() { is_pushdown_scalar_aggr_ = true; }
  bool is_pushdown_scalar_aggr() { return is_pushdown_scalar_aggr_; }

  VIRTUAL_TO_STRING_KV(K_(group_exprs), K_(rollup_exprs), K_(aggr_exprs), K_(algo),
      K_(is_push_down));
  virtual int get_card_without_filter(double &card) override;

  int set_hash_rollup_info(const ObHashRollupInfo &info) { return hash_rollup_info_.assign(info); }
  const ObHashRollupInfo *get_hash_rollup_info() const
  {
    return hash_rollup_info_.valid() ? &hash_rollup_info_ : nullptr;
  }

private:
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
  virtual int allocate_granule_post(AllocGIContext &ctx) override;
  virtual int allocate_granule_pre(AllocGIContext &ctx) override;
  int create_fd_item_from_select_list(ObFdItemSet *fd_item_set);
  virtual int compute_one_row_info() override;
  virtual int print_outline_data(PlanText &plan_text) override;
  virtual int print_used_hint(PlanText &plan_text) override;
  virtual int check_use_child_ordering(bool &used, int64_t &inherit_child_ordering_index)override;
  virtual int compute_op_parallel_and_server_info() override;
private:
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> group_exprs_;
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> rollup_exprs_;
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> aggr_exprs_;
  AggregateAlgo algo_;
  bool from_pivot_;
  bool is_push_down_;
  bool is_partition_gi_;
  double total_ndv_;
  double origin_child_card_;

  ObThreeStageAggrInfo three_stage_info_;
  // for rollup distributor and collector
  ObRollupAdaptiveInfo rollup_adaptive_info_;
  bool force_push_down_; // control by _aggregation_optimization_settings
  // use print outline
  bool use_hash_aggr_;
  bool has_push_down_;
  bool use_part_sort_;
  ObItemType dist_method_;
  int64_t gby_dop_;
  // end use print outline
  bool is_pushdown_scalar_aggr_;
  ObHashRollupInfo hash_rollup_info_;
};
} // end of namespace sql
} // end of namespace oceanbase

#endif // OCEANBASE_SQL_OB_LOG_GROUP_BY_H
