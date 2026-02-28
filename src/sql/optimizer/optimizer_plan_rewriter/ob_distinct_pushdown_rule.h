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

#ifndef OCEANBASE_SQL_OB_PUSH_DOWN_DISTINCT_RULE_H
#define OCEANBASE_SQL_OB_PUSH_DOWN_DISTINCT_RULE_H

#include "sql/optimizer/optimizer_plan_rewriter/ob_optimize_rule.h"
#include "sql/optimizer/optimizer_plan_rewriter/ob_plan_visitor.h"

namespace oceanbase
{
namespace sql
{

// Forward declarations
class ObLogPlan;
class ObLogGroupBy;
class ObLogJoin;
class ObLogicalOperator;
class ObOptimizerContext;

/**
 * @brief Distinct 下推优化规则
 * 实现将 Distinct 下推到 JOIN 下方的优化
 */
class ObDistinctPushdownRule : public ObOptimizeRule
{
public:
  ObDistinctPushdownRule() {}
  ~ObDistinctPushdownRule() {}

  /**
   * @brief 应用规则
   * @param root_plan 根计划节点
   * @param ctx 优化器上下文
   * @param result 规则结果
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  virtual int apply_rule(ObLogPlan *root_plan,
                        ObOptimizerContext &ctx,
                        ObRuleResult &result) override;

  /**
   * @brief 检查规则是否启用
   * @param ctx 优化器上下文
   * @return true 如果启用，false 否则
   */
  virtual bool is_enabled(const ObOptimizerContext &ctx) override;
};

/**
 * @brief Distinct 下推规则上下文
 * 用于在遍历计划树时传递上下文信息
 */
struct ObDistinctPushdownContext : public RewriterContext
{
  ObDistinctPushdownContext()
      : RewriterContext(), enable_reshuffle_(false), enable_place_distinct_(false), benefit_from_reshuffle_(false)
  {}
  virtual ~ObDistinctPushdownContext() {}

  bool is_empty() const { return distinct_exprs_.empty(); }

  void reset() {
    distinct_exprs_.reset();
    enable_reshuffle_ = false;
    enable_place_distinct_ = false;
    benefit_from_reshuffle_ = false;
  }

  int merge_with(const common::ObIArray<ObRawExpr*> &lower_distinct_exprs,
                 bool &should_update,
                 bool &ignore_context,
                 bool &pushed_down);

  int assign(const ObDistinctPushdownContext &other);
  int map_and_check(const common::ObIArray<ObRawExpr *> &from_exprs,
                    const common::ObIArray<ObRawExpr *> &to_exprs,
                    ObRawExprFactory &expr_factory,
                    const ObSQLSessionInfo *session_info,
                    bool check_valid,
                    bool &is_valid);
  int map(const common::ObIArray<ObRawExpr *> &from_exprs,
          const common::ObIArray<ObRawExpr *> &to_exprs,
          ObRawExprFactory &expr_factory,
          const ObSQLSessionInfo *session_info);

  // The distinct expressions to be pushed down.
  common::ObSEArray<ObRawExpr*, 4> distinct_exprs_;

  // Whether reshuffle is allowed above current operator.
  // This depends on whether upper operators require the current sharding.
  // At exchange operators, this flag is reset to true, because exchange redefines the sharding.
  bool enable_reshuffle_;

  // Whether distinct placement is allowed above the current operator.
  // This flag defaults to false when the context is created, and is set to true only when a costly operator
  // (such as JOIN or EXCHANGE) has been traversed during pushdown. This enables placement of a new
  // distinct operator for data reduction after expensive operators.
  bool enable_place_distinct_;

  // Generally speaking, since the nature of distinct pushing through Join,
  // Hash/Exchange operators can provide the required sharding for distinct.
  // When there are Hash/Exchange operators above the current operator,
  // and no high-cost operators are encountered during the pushdown,
  // we should disable reshuffle to avoid unnecessary shuffle operations.
  bool benefit_from_reshuffle_;

  TO_STRING_KV(K_(distinct_exprs), K_(enable_reshuffle), K_(enable_place_distinct), K_(benefit_from_reshuffle));
};

struct ObDistinctPushdownResult {
  ObDistinctPushdownResult()
  : is_materialized_(false)
  { }

  virtual ~ObDistinctPushdownResult() {}

  inline bool is_materialized() const { return is_materialized_; }

  int build_from(const ObDistinctPushdownResult* &copied_from_result);

  int assign(const ObDistinctPushdownResult &other)
  {
    is_materialized_ = other.is_materialized_;
    return OB_SUCCESS;
  }

  inline void set_is_materialized(bool is_materialized)
  { is_materialized_ = is_materialized; }


  bool is_materialized_;
  TO_STRING_KV(K_(is_materialized));
};

/**
 * @brief Distinct 下推规则的访问者
 * 用于遍历计划树并识别可以下推的 Distinct 节点
 */
class ObDistinctPushDownPlanRewriter : public SimplePlanRewriter<ObDistinctPushdownResult, ObDistinctPushdownContext>
{
public:
  ObDistinctPushDownPlanRewriter() = delete;
  ObDistinctPushDownPlanRewriter(ObOptimizerContext &opt_ctx) : opt_ctx_(opt_ctx), transform_happened_(false) {}
  virtual ~ObDistinctPushDownPlanRewriter() {}

  bool transform_happened() const { return transform_happened_; }

  /**
   * @brief 访问 Distinct 节点
   * 检查是否可以下推到 JOIN 下方
   */
  virtual int visit_distinct(ObLogDistinct *distinct,
                            ObDistinctPushdownContext *context,
                            ObDistinctPushdownResult *&result) override;

  /**
   * @brief 访问 Distinct 节点
   * 检查是否可以下推到 JOIN 下方
   */
  virtual int visit_groupby(ObLogGroupBy *groupby,
                            ObDistinctPushdownContext *context,
                            ObDistinctPushdownResult *&result) override;

  /**
   * @brief 访问 JOIN 节点
   * 检查是否可以接受 Distinct 下推
   */
  virtual int visit_join(ObLogJoin *join,
                         ObDistinctPushdownContext *context,
                         ObDistinctPushdownResult *&result) override;

  /**
   * @brief 访问 Exchange 节点
   * 检查是否可以接受 Distinct 下推
   */
  virtual int visit_exchange(ObLogExchange *exchange,
                             ObDistinctPushdownContext *ctx,
                             ObDistinctPushdownResult *&result) override;

  /**
   * @brief 访问 Material 节点
   * 检查是否可以接受 Distinct 下推
   */
  virtual int visit_material(ObLogMaterial *material,
                             ObDistinctPushdownContext *ctx,
                             ObDistinctPushdownResult *&result) override;

  /**
   * @brief 访问 TableScan 节点
   * 检查是否可以接受 Distinct 下推
   */
  virtual int visit_table_scan(ObLogTableScan *table_scan,
                               ObDistinctPushdownContext *ctx,
                               ObDistinctPushdownResult *&result) override;

  /**
   * @brief 访问 SubPlanScan 节点
   * 检查是否可以接受 Distinct 下推
   */
  virtual int visit_subplan_scan(ObLogSubPlanScan *subplan_scan,
                                 ObDistinctPushdownContext *ctx,
                                 ObDistinctPushdownResult *&result) override;

  /**
   * @brief 访问 Set 节点
   * 检查是否可以接受 Distinct 下推
   */
  virtual int visit_set(ObLogSet *set,
                        ObDistinctPushdownContext *ctx,
                        ObDistinctPushdownResult *&result) override;

  /**
   * @brief 访问通用节点
   */
  virtual int visit_node(ObLogicalOperator *op,
                         ObDistinctPushdownContext *context,
                         ObDistinctPushdownResult *&result) override;

private:
  /**
   * @brief 触发子节点的改写，本身不做任何处理。
   * @param op 当前节点
   * @param result 结果
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int default_rewrite_children(ObLogicalOperator *op, ObDistinctPushdownResult *&result);

  /**
   * @brief 触发子节点的改写，本身不做任何处理。
   * @param child_op 子节点
   * @param result 结果
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int default_rewrite_child(ObLogicalOperator *child_op, ObDistinctPushdownResult *&result);

private:
  /**
   * @brief 检查 Distinct 是否可以下推
   * @param distinct Distinct 节点
   * @param can_push 是否可以下推
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int can_pushdown_distinct(ObLogDistinct *distinct, bool &can_push);

  /**
   * @brief 生成上下文
   * @param distinct Distinct 节点
   * @param context 出参，生成的上下文
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int generate_context(ObLogDistinct *distinct, ObDistinctPushdownContext &ctx);

  /**
   * @brief 生成上下文
   * @param groupby GroupBy 节点
   * @param context 出参，生成的上下文
   * @param can_pushdown 是否可以下推
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int generate_context_from_groupby(ObLogGroupBy *groupby, ObDistinctPushdownContext &ctx, bool &can_pushdown);

  /**
   * @brief 检查是否可以通过 Exchange 下推
   * @param exchange Exchange 节点
   * @param ctx 上下文
   * @param can_push 是否可以下推
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int can_push_through_exchange(ObLogExchange *exchange, ObDistinctPushdownContext *ctx, bool &can_push);

  /**
   * @brief 设置上下文标志位
   * @param exchange Exchange 节点
   * @param ctx 上下文
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int set_context_flags_for_exchange(ObLogExchange *exchange, ObDistinctPushdownContext *ctx);

  /**
   * @brief 执行放置distinct
   * @param op 当前节点
   * @param ctx 上下文
   * @param result 结果
   * @param ndv 不同值数量
   * @param need_reshuffle 是否需要重shuffle
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int do_place_distinct(ObLogicalOperator *op,
                      //  ObDistinctPushdownContext *ctx,
                       ObSEArray<ObRawExpr *, 4> &distinct_exprs,
                       ObDistinctPushdownResult *result,
                       double ndv,
                       bool need_reshuffle);

  /**
   * @brief 尝试放置distinct
   * @param op 当前节点
   * @param ctx 上下文
   * @param result 结果
   * @return OB_SUCCESS 成功，其他值表示失败
   * @note 放置后需要更新parent的child
   */
  int try_place_distinct(ObLogicalOperator *op, ObDistinctPushdownContext *ctx, ObDistinctPushdownResult *result);

  int simplify_distinct_exprs(ObLogicalOperator *op,
                              ObIArray<ObRawExpr *> &distinct_exprs,
                              ObSEArray<ObRawExpr *, 4> &simplified_exprs);

  int filter_distinct_exprs_by(ObDistinctPushdownContext *context,
                               ObLogicalOperator *op,
                               ObLogicalOperator *join_op,
                               const common::ObIArray<ObRawExpr*> &join_exprs,
                               bool &can_push,
                               bool &is_partially_pushdown);

  /*
  * this will not work for union as relation id is merged from all children
  * this only works for join to check join side
  * if we only want to push to one side
  */
  int all_distinct_exprs_depdend_on(const ObIArray<ObRawExpr *> &exprs, ObLogicalOperator *&op, bool &is_true);

private:
  ObOptimizerContext &opt_ctx_;
  bool transform_happened_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_PUSH_DOWN_DISTINCT_RULE_H
