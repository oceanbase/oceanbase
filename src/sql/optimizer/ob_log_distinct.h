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

#ifndef OCEANBASE_SQL_OPTIMITZER_OB_LOG_DISTINCT_
#define OCEANBASE_SQL_OPTIMITZER_OB_LOG_DISTINCT_
#include "lib/container/ob_se_array.h"
#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase
{
namespace sql
{
class ObLogDistinct : public ObLogicalOperator
{
public:
  ObLogDistinct(ObLogPlan &plan)
      : ObLogicalOperator(plan),
        algo_(AGGREGATE_UNINITIALIZED),
        is_block_mode_(false),
        is_push_down_(false),
        total_ndv_(-1.0),
        force_push_down_(false),
        input_sorted_(false)
  { }
  virtual ~ObLogDistinct()
  { }

  const char *get_name() const;
  //this interface can be used for adding distinct expr
  inline ObIArray<ObRawExpr*>& get_distinct_exprs()
  { return distinct_exprs_; }
  inline const ObIArray<ObRawExpr*>& get_distinct_exprs() const
  { return distinct_exprs_; }
  int set_distinct_exprs(const common::ObIArray<ObRawExpr*> &exprs)
  { return append(distinct_exprs_, exprs); }
  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
  virtual uint64_t hash(uint64_t seed) const override;

  inline void set_hash_type() { algo_ = HASH_AGGREGATE; }
  inline void set_merge_type() { algo_ = MERGE_AGGREGATE; }
  inline void set_algo_type(AggregateAlgo type) { algo_ = type; }
  inline AggregateAlgo get_algo() const { return algo_; }
  inline void set_block_mode(bool is_block_mode) { is_block_mode_ = is_block_mode; }
  inline bool get_block_mode() { return is_block_mode_; }
  virtual int est_cost() override;
  virtual int est_width() override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
  int inner_est_cost(const int64_t parallel, double child_card, double child_ndv, double &op_cost);
  virtual bool is_block_op() const override { return false; }
  virtual int compute_fd_item_set() override;
  virtual int allocate_granule_post(AllocGIContext &ctx) override;
  virtual int allocate_granule_pre(AllocGIContext &ctx) override;
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int compute_op_ordering() override;
  inline bool is_push_down() const { return is_push_down_; }
  inline void set_push_down(const bool is_push_down) { is_push_down_ = is_push_down; }
  inline double get_total_ndv() const { return total_ndv_; }
  inline void set_total_ndv(double total_ndv) { total_ndv_ = total_ndv; }
  inline bool force_partition_gi() const { return (is_partition_wise() && !is_push_down()) || is_partition_gi_; }
  inline bool force_push_down() const { return force_push_down_; }
  inline void set_force_push_down(bool force_push_down) { force_push_down_ = force_push_down; }
  int get_distinct_output_exprs(ObIArray<ObRawExpr *> &output_exprs);
  virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
  virtual int print_outline_data(PlanText &plan_text) override;
  virtual int print_used_hint(PlanText &plan_text) override;
  inline bool is_partition_ig() const { return is_partition_gi_; }
  inline void set_is_partition_gi(bool v) { is_partition_gi_ = v; }
  virtual int get_card_without_filter(double &card) override;
  virtual int check_use_child_ordering(bool &used, int64_t &inherit_child_ordering_index)override;
  virtual int compute_property() override;

private:
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> distinct_exprs_;
  AggregateAlgo algo_;
  bool is_block_mode_;
  bool is_push_down_;
  double total_ndv_;
  bool force_push_down_; // control by _aggregation_optimization_settings
  bool is_partition_gi_;
  bool input_sorted_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogDistinct);
};

}
}
#endif
