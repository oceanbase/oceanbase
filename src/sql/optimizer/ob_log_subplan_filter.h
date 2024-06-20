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

#ifndef OCEANBASE_SQL_OB_LOG_SUBPLAN_FILTER_H_
#define OCEANBASE_SQL_OB_LOG_SUBPLAN_FILTER_H_

#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase
{
namespace sql
{
struct ObBasicCostInfo;
class ObLogSubPlanFilter : public ObLogicalOperator
{
public:
  ObLogSubPlanFilter(ObLogPlan &plan)
      : ObLogicalOperator(plan),
        dist_algo_(DIST_INVALID_METHOD),
        subquery_exprs_(),
        exec_params_(),
        onetime_exprs_(),
        init_plan_idxs_(),
        one_time_idxs_(),
        update_set_(false),
        enable_das_group_rescan_(false)
  {}
  ~ObLogSubPlanFilter() {}
  virtual int est_cost() override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
  // re est children cost and gather cost infos
  int get_re_est_cost_infos(const EstimateCostInfo &param, ObIArray<ObBasicCostInfo> &cost_infos);

  inline int add_subquery_exprs(const ObIArray<ObQueryRefRawExpr *> &query_exprs)
  {
    return append(subquery_exprs_, query_exprs);
  }

  inline int add_exec_params(const ObIArray<ObExecParamRawExpr *> &exec_params)
  {
    return append(exec_params_, exec_params);
  }
  /**
   *  Get the exec params
   */
  inline const common::ObIArray<ObExecParamRawExpr*> &get_exec_params() const { return exec_params_; }

  inline common::ObIArray<ObExecParamRawExpr *> &get_exec_params() { return exec_params_; }

  inline bool has_exec_params() const { return !exec_params_.empty(); }

  inline const common::ObIArray<ObExecParamRawExpr *> &get_onetime_exprs() const { return onetime_exprs_; }

  inline common::ObIArray<ObExecParamRawExpr *> &get_onetime_exprs() { return onetime_exprs_; }

  inline int add_onetime_exprs(const common::ObIArray<ObExecParamRawExpr*> &onetime_exprs)
  {
    return append(onetime_exprs_, onetime_exprs);
  }

  inline const common::ObBitSet<> &get_onetime_idxs() { return one_time_idxs_; }

  inline int add_onetime_idxs(const common::ObBitSet<> &one_time_idxs) { return one_time_idxs_.add_members(one_time_idxs); }

  inline const common::ObBitSet<> &get_initplan_idxs() { return init_plan_idxs_; }

  inline int add_initplan_idxs(const common::ObBitSet<> &init_plan_idxs) { return init_plan_idxs_.add_members(init_plan_idxs); }

  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;

  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;

  bool is_my_subquery_expr(const ObQueryRefRawExpr *query_expr);
  bool is_my_exec_expr(const ObRawExpr *expr);
  bool is_my_onetime_expr(const ObRawExpr *expr);

  int get_exists_style_exprs(ObIArray<ObRawExpr*> &subquery_exprs);

  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;

  void set_update_set(bool update_set)
  { update_set_ = update_set; }
  bool is_update_set() { return update_set_; }
  int allocate_granule_pre(AllocGIContext &ctx);
  int allocate_granule_post(AllocGIContext &ctx);
  virtual int compute_one_row_info() override;
  virtual int compute_sharding_info() override;
  inline DistAlgo get_distributed_algo() { return dist_algo_; }
  inline void set_distributed_algo(const DistAlgo set_dist_algo) { dist_algo_ = set_dist_algo; }

  int add_px_batch_rescan_flag(bool flag) { return enable_px_batch_rescans_.push_back(flag); }
  common::ObIArray<bool> &get_px_batch_rescans() {  return enable_px_batch_rescans_; }

  inline bool enable_das_group_rescan() { return enable_das_group_rescan_; }
  inline void set_enable_das_group_rescan(bool flag) { enable_das_group_rescan_ = flag; }
  int check_and_set_das_group_rescan();
  int check_if_match_das_group_rescan(ObLogicalOperator *root, bool &group_rescan);
  int set_use_das_batch(ObLogicalOperator* root);

  int allocate_startup_expr_post() override;

  int allocate_startup_expr_post(int64_t child_idx) override;

  int allocate_subquery_id();

  int replace_nested_subquery_exprs(ObRawExprReplacer &replacer);
  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;

  common::ObIArray<ObExecParamRawExpr *> &get_above_pushdown_left_params() { return above_pushdown_left_params_; }

  common::ObIArray<ObExecParamRawExpr *> &get_above_pushdown_right_params() { return above_pushdown_right_params_; }

  int get_repart_sharding_info(ObLogicalOperator* child_op,
                               ObShardingInfo *&strong_sharding,
                               ObIArray<ObShardingInfo*> &weak_sharding);

  int rebuild_repart_sharding_info(const ObShardingInfo *input_sharding,
                                   ObIArray<ObRawExpr*> &src_keys,
                                   ObIArray<ObRawExpr*> &target_keys,
                                   EqualSets &input_esets,
                                   ObShardingInfo *&out_sharding);

  virtual int compute_op_parallel_and_server_info() override;
  virtual int print_outline_data(PlanText &plan_text) override;
  virtual int print_used_hint(PlanText &plan_text) override;
  virtual int open_px_resource_analyze(OPEN_PX_RESOURCE_ANALYZE_DECLARE_ARG) override;
  virtual int close_px_resource_analyze(CLOSE_PX_RESOURCE_ANALYZE_DECLARE_ARG) override;
private:
  int extract_exist_style_subquery_exprs(ObRawExpr *expr,
                                         ObIArray<ObRawExpr*> &exist_style_exprs);
  int check_expr_contain_row_subquery(const ObRawExpr *expr,
                                         bool &contains);
  int get_sub_qb_names(ObIArray<ObString>& sub_qb_names);
protected:
  DistAlgo dist_algo_;
  common::ObSEArray<ObQueryRefRawExpr *, 8, common::ModulePageAllocator, true> subquery_exprs_;
  common::ObSEArray<ObExecParamRawExpr *, 8, common::ModulePageAllocator, true> exec_params_;
  common::ObSEArray<ObExecParamRawExpr *, 8, common::ModulePageAllocator, true> onetime_exprs_;

  //InitPlan idxs，InitPlan只算一次，需要存储结果
  common::ObBitSet<> init_plan_idxs_;
  //One-Time idxs，One-Time只算一次，不用存储结果
  common::ObBitSet<> one_time_idxs_;
  bool update_set_;
  common::ObSEArray<bool , 8, common::ModulePageAllocator, true> enable_px_batch_rescans_;

  common::ObSEArray<ObExecParamRawExpr *, 4, common::ModulePageAllocator, true> above_pushdown_left_params_;
  common::ObSEArray<ObExecParamRawExpr *, 4, common::ModulePageAllocator, true> above_pushdown_right_params_;
  bool enable_das_group_rescan_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogSubPlanFilter);
};
} // end of namespace sql
} // end of namespace oceanbase


#endif // OCEANBASE_SQL_OB_LOG_SUBPLAN_FILTER_H_
