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

#ifndef OCEANBASE_SQL_OB_LOG_WINDOW_FUNCTION_H
#define OCEANBASE_SQL_OB_LOG_WINDOW_FUNCTION_H
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_set.h"
namespace oceanbase
{
namespace sql
{
  class ObLogWindowFunction : public ObLogicalOperator
  {
  public:
    enum WindowFunctionRoleType
    {
      NORMAL = 0,
      PARTICIPATOR = 1, // for wf adaptive pushdown
      CONSOLIDATOR = 2 // for wf adaptive pushdown
    };
    ObLogWindowFunction(ObLogPlan &plan)
        : ObLogicalOperator(plan),
        algo_(WinDistAlgo::WIN_DIST_INVALID),
        use_hash_sort_(false),
        single_part_parallel_(false),
        range_dist_parallel_(false),
        role_type_(WindowFunctionRoleType::NORMAL),
        rd_sort_keys_cnt_(0),
        rd_pby_sort_cnt_(0),
        wf_aggr_status_expr_(NULL)
    {}
    virtual ~ObLogWindowFunction() {}
    virtual int get_explain_name_internal(char *buf,
                                          const int64_t buf_len,
                                          int64_t &pos);
    inline void set_aggr_status_expr(ObOpPseudoColumnRawExpr *wf_aggr_status_expr)
    { wf_aggr_status_expr_ = wf_aggr_status_expr; }
    inline ObOpPseudoColumnRawExpr *&get_aggr_status_expr() { return wf_aggr_status_expr_; }
    inline int add_window_expr(ObWinFunRawExpr *win_expr)
    { return win_exprs_.push_back(win_expr); }
    inline ObIArray<ObWinFunRawExpr *> &get_window_exprs() { return win_exprs_; }
    inline const ObIArray<ObWinFunRawExpr *> &get_window_exprs() const { return win_exprs_; }
    virtual uint64_t hash(uint64_t seed) const override;
    virtual int est_cost() override;
    virtual int est_width() override;
    virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
    virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
    virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
    virtual int compute_op_ordering() override;
    virtual int compute_sharding_info() override;
    virtual bool is_block_op() const override;
    virtual int allocate_granule_post(AllocGIContext &ctx) override;
    virtual int allocate_granule_pre(AllocGIContext &ctx) override;
    int get_win_partition_intersect_exprs(ObIArray<ObWinFunRawExpr *> &win_exprs,
                                          ObIArray<ObRawExpr *> &win_part_exprs);
    virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;
    void set_single_part_parallel(bool v) { single_part_parallel_ = v; }
    bool is_single_part_parallel() const { return single_part_parallel_; }
    void set_ragne_dist_parallel(bool v) { range_dist_parallel_ = v; }
    bool is_range_dist_parallel() const { return range_dist_parallel_; }
    int get_winfunc_output_exprs(ObIArray<ObRawExpr *> &output_exprs);
    void set_role_type(WindowFunctionRoleType v) { role_type_ = v; }
    WindowFunctionRoleType get_role_type() const { return role_type_; }
    bool is_push_down() const { return PARTICIPATOR == role_type_|| CONSOLIDATOR == role_type_; }
    bool is_participator() const { return PARTICIPATOR == role_type_; }
    bool is_consolidator() const { return CONSOLIDATOR == role_type_; }
    int get_rd_sort_keys(common::ObIArray<OrderItem> &rd_sort_keys);
    int set_sort_keys(const common::ObIArray<OrderItem> &sort_keys)
    {
      return sort_keys_.assign(sort_keys);
    }
    const common::ObIArray<OrderItem> &get_sort_keys() const
    {
      return sort_keys_;
    }
    void set_rd_sort_keys_cnt(const int64_t cnt) { rd_sort_keys_cnt_ = cnt; }
    int set_pushdown_info(const common::ObIArray<bool> &pushdown_info)
    {
      return pushdown_info_.assign(pushdown_info);
    }
    const common::ObIArray<bool> &get_pushdown_info() const
    {
      return pushdown_info_;
    }
    void set_rd_pby_sort_cnt(const int64_t cnt) { rd_pby_sort_cnt_ = cnt; }
    int64_t get_rd_pby_sort_cnt() const { return rd_pby_sort_cnt_; }

    void set_win_dist_algo(const WinDistAlgo algo)  { algo_ = algo; }
    WinDistAlgo get_win_dist_algo() const { return algo_; }
    void set_use_hash_sort(const bool use_hash_sort)  { use_hash_sort_ = use_hash_sort; }
    bool get_use_hash_sort() const { return use_hash_sort_; }
    virtual int get_plan_item_info(PlanText &plan_text,
                                ObSqlPlanItem &plan_item) override;
    virtual int print_outline_data(PlanText &plan_text) override;
    virtual int print_used_hint(PlanText &plan_text) override;
    int add_win_dist_options(const ObLogicalOperator *op,
                             const ObIArray<ObWinFunRawExpr*> &all_win_funcs,
                             ObWindowDistHint &win_dist_hint);
  private:
    ObSEArray<ObWinFunRawExpr *, 4, common::ModulePageAllocator, true> win_exprs_;

    // for print PQ_DISTRIBUTE_WINDOW hint outline
    WinDistAlgo algo_;
    bool use_hash_sort_;

    // Single partition (no partition by) window function parallel process, need the PX COORD
    // to collect the partial result and broadcast the final result to each worker.
    // Enable condition:
    // 1. Only one partition (no partition by)
    // 2. Window is the whole partition
    // 3. Only the following functions supported: sum,count,max,min
    bool single_part_parallel_;

    // Range distribution window function parallel process: data is range distributed,
    // the first and last partition of each worker may be partial result. Then the PX COORD
    // calculate the `patch` of the first row of first partition and last row of last
    // partition, finally the other rows of those partition will be updated (apply the patch).
    // Enable condition:
    // 1. NDV of partition by is too small for parallelism
    // 2. Is cumulative window, window is from first row to current row
    // 3. Only the following functions supported: rank,dense_rank,sum,count,min,max
    bool range_dist_parallel_;

    //
    WindowFunctionRoleType role_type_;

    // sort keys needed for window function
    common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> sort_keys_;
    // sort keys count for range distributed parallel.
    int64_t rd_sort_keys_cnt_;
    // the first %rd_pby_sort_cnt_ of %rd_sort_keys_ is the partition by of window function.
    int64_t rd_pby_sort_cnt_;

    // for reporting window function adaptive pushdown
    ObOpPseudoColumnRawExpr *wf_aggr_status_expr_;
    common::ObSEArray<bool, 8, common::ModulePageAllocator, true> pushdown_info_;
  };
}
}
#endif // OCEANBASE_SQL_OB_LOG_WINDOW_FUNCTION_H
