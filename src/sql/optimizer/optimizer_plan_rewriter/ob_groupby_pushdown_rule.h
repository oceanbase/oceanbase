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

#ifndef OCEANBASE_SQL_OB_PUSH_DOWN_GROUPBY_RULE_H
#define OCEANBASE_SQL_OB_PUSH_DOWN_GROUPBY_RULE_H

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
 * @brief GroupBy 下推优化规则
 * 实现将 GroupBy 下推到 JOIN 下方的优化
 */
class ObGroupByPushdownRule : public ObOptimizeRule
{
public:
  ObGroupByPushdownRule() {}
  ~ObGroupByPushdownRule() {}

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
 * @brief GroupBy pushdown rule context
 * Used for passing context information when traversing the plan tree.
 * The context is created at the GroupBy operator that initiates a pushdown attempt,
 * and sub-contexts are generated when the pushdown crosses operators like Exchange, Join, or Set.
 * Corresponds one-to-one with ObGroupByPushdownResult.
 */
struct ObGroupByPushdownContext : public RewriterContext
{

  struct MaskAggrExprInfo
  {
    MaskAggrExprInfo() : aggr_expr_(NULL), new_param_expr_(NULL) {}
    virtual ~MaskAggrExprInfo() {}
    ObRawExpr *aggr_expr_;
    ObRawExpr *param_expr_;
    ObRawExpr *new_param_expr_;
    TO_STRING_KV(K_(aggr_expr), K_(new_param_expr));
  };

  ObGroupByPushdownContext()
      : RewriterContext(),
        need_count_(false),
        enable_reshuffle_(false),
        enable_place_groupby_(false),
        benefit_from_reshuffle_(false)
  {}
  virtual ~ObGroupByPushdownContext() {}

  bool is_empty() const { return groupby_exprs_.empty() && aggr_exprs_.empty() && !need_count_; }

  void reset() {
    groupby_exprs_.reset();
    aggr_exprs_.reset();
    need_count_ = false;
    enable_reshuffle_ = false;
    enable_place_groupby_ = false;
    benefit_from_reshuffle_ = false;
  }

  /**
   * @brief 查找是否已经有包含相同 param_expr 的 mask_info
   * @param param_expr 要查找的参数表达式
   * @param new_param_expr 如果找到，返回对应的 new_param_expr
   * @return true 如果找到，false 否则
   */
  bool find_existing_mask_aggr_param_expr(ObRawExpr *param_expr, ObRawExpr *&new_param_expr) const
  {
    bool found = false;
    new_param_expr = NULL;
    if (OB_NOT_NULL(param_expr)) {
      for (int64_t i = 0; !found && i < mask_aggr_infos_.count(); ++i) {
        const MaskAggrExprInfo &mask_info = mask_aggr_infos_.at(i);
        if (mask_info.param_expr_ == param_expr) {
          new_param_expr = mask_info.new_param_expr_;
          found = true;
        }
      }
    }
    return found;
  }

  int assign(const ObGroupByPushdownContext &other);
  int get_aggr_items(common::ObIArray<ObAggFunRawExpr*> &aggr_items);
  int get_dependent_exprs(common::ObIArray<ObRawExpr*> &dependent_exprs);
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

  // The group by expressions to be pushed down.
  common::ObSEArray<ObRawExpr*, 4> groupby_exprs_;

  // The aggregate expressions to be pushed down.
  common::ObSEArray<ObRawExpr*, 4> aggr_exprs_;

  // The mask aggregate expressions to be pushed down.
  // mask aggr expr is like sum(case when c1 > 0 then c2 else null end)
  // it can be pushed down as sum(c2), and rewritten to sum(case when c1 > 0 then sum(c2) else null end) (add c1 to groupby exprs)
  common::ObArray<MaskAggrExprInfo> mask_aggr_infos_;

  // Whether the count(*) expression is needed.
  // count(*) is needed when pushing down sum / count functions through join.
  bool need_count_;

  // Whether reshuffle is allowed above current operator.
  // This depends on whether upper operators require the current sharding.
  // At exchange operators, this flag is reset to true, because exchange redefines the sharding.
  bool enable_reshuffle_;

  // Whether place groupby is allowed above the current operator.
  // This flag defaults to false when the context is created, and is set to true only when a costly operator
  // (such as JOIN or EXCHANGE) has been traversed during pushdown. This enables placement of a new
  // groupby operator for data reduction after expensive operators.
  // enable place groupby only when pushdown through costive op like join, exchange, etc.
  bool enable_place_groupby_;

  // Generally speaking, since the nature of groupby pushing through Join,
  // Hash/Exchange operators can provide the required sharding for groupby.
  // When there are Hash/Exchange operators above the current operator,
  // and no high-cost operators are encountered during the pushdown,
  // we should disable reshuffle to avoid unnecessary shuffle operations.
  bool benefit_from_reshuffle_;

  TO_STRING_KV(K_(groupby_exprs),
               K_(aggr_exprs),
               K_(need_count),
               K_(enable_reshuffle),
               K_(enable_place_groupby),
               K_(benefit_from_reshuffle));
};

/**
 * @brief Result of GroupBy pushdown, must correspond one-to-one with Context
 */
struct ObGroupByPushdownResult : public RewriterResult
{
  ObGroupByPushdownResult()
    : RewriterResult()
    , is_materialized_(false)
    , new_aggr_exprs_()
    , count_expr_(NULL)
  {}

  void reset() {
    is_materialized_ = false;
    new_aggr_exprs_.reset();
    count_expr_ = NULL;
  }

  // Whether the groupby pushdown is materialized.
  bool is_materialized_;

  // Stores the pre-aggregation results for each aggr_expr from ctx if groupby is pushed down.
  // These results may not always be aggregate expressions (e.g., pushing down sum(a) to join
  // may store sum(a) * count(a), and for subplan scan pushdown, may store column expressions).
  common::ObSEArray<ObRawExpr*, 4> new_aggr_exprs_;

  // Pre-aggregation result for count(*) expression if required by the context.
  // Note: Similar to new_aggr_exprs_, this may contain expressions that are not
  // actual aggregate functions.
  ObRawExpr *count_expr_;

  TO_STRING_KV(K_(is_materialized), K_(new_aggr_exprs), K_(count_expr));
};

/**
 * @brief GroupBy 下推规则的访问者
 * 用于遍历计划树并识别可以下推的 GroupBy 节点
 * TODO tuliwei.tlw: 补充注释，以及翻译成英文
 */
class ObGroupByPushDownPlanRewriter : public SimplePlanRewriter<ObGroupByPushdownResult, ObGroupByPushdownContext>
{
public:
  ObGroupByPushDownPlanRewriter() = delete;
  ObGroupByPushDownPlanRewriter(ObOptimizerContext &opt_ctx) : opt_ctx_(opt_ctx), transform_happened_(false) {}
  virtual ~ObGroupByPushDownPlanRewriter() {}

  bool transform_happened() const { return transform_happened_; }

  /**
   * @brief visit groupby node
   // Check whether this GroupBy node can initiate a GroupBy pushdown.
   // If so, generate a new context from this node and propagate
   // it downward to start the pushdown process.
   // Note: Even if a context is provided from above, we currently
   // do not attempt to place a GroupBy above this node using an
   // upstream context.
   */
  virtual int visit_groupby(ObLogGroupBy *groupby,
                            ObGroupByPushdownContext *context,
                            ObGroupByPushdownResult *&result) override;

  /**
   * @brief 访问 JOIN 节点
   * 检查是否可以接受 GroupBy 下推
   */
  virtual int visit_join(ObLogJoin *join,
                         ObGroupByPushdownContext *context,
                         ObGroupByPushdownResult *&result) override;

  /**
   * @brief 访问 Exchange 节点
   * 检查是否可以接受 GroupBy 下推
   */
  virtual int visit_exchange(ObLogExchange *exchange,
                             ObGroupByPushdownContext *ctx,
                             ObGroupByPushdownResult *&result) override;

  /**
   * @brief 访问 Material 节点
   * 检查是否可以接受 GroupBy 下推
   */
  virtual int visit_material(ObLogMaterial *material,
                             ObGroupByPushdownContext *ctx,
                             ObGroupByPushdownResult *&result) override;

  /**
   * @brief 访问 TableScan 节点
   * 检查是否可以接受 GroupBy 下推
   */
  virtual int visit_table_scan(ObLogTableScan *table_scan,
                               ObGroupByPushdownContext *ctx,
                               ObGroupByPushdownResult *&result) override;

  /**
   * @brief 访问 SubPlanScan 节点
   * 检查是否可以接受 GroupBy 下推
   */
  virtual int visit_subplan_scan(ObLogSubPlanScan *subplan_scan,
                                 ObGroupByPushdownContext *ctx,
                                 ObGroupByPushdownResult *&result) override;

  /**
   * @brief 访问 Set 节点
   * 检查是否可以接受 GroupBy 下推
   */
  virtual int visit_set(ObLogSet *set,
                        ObGroupByPushdownContext *ctx,
                        ObGroupByPushdownResult *&result) override;

  /**
   * @brief 访问通用节点
   */
  virtual int visit_node(ObLogicalOperator *op,
                         ObGroupByPushdownContext *context,
                         ObGroupByPushdownResult *&result) override;

private:
  /**
   * @brief 触发子节点的改写，本身不做任何处理。
   * @param op 当前节点
   * @param result 结果
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int default_rewrite_children(ObLogicalOperator *op, ObGroupByPushdownResult *result);

  /**
   * @brief 触发子节点的改写，本身不做任何处理。
   * @param op 当前节点
   * @param result 结果
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int default_rewrite_child(ObLogicalOperator *child_op, ObGroupByPushdownResult *result);

private:
  /**
   * @brief 检查 GroupBy 是否可以下推
   * @param groupby GroupBy 节点
   * @param can_push 是否可以下推
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int can_pushdown_groupby(ObLogGroupBy *groupby, bool &can_push);

  /**
   * @brief 生成上下文
   * @param groupby GroupBy 节点
   * @param context 出参，生成的上下文
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int generate_context(ObLogGroupBy *groupby, ObGroupByPushdownContext *upper_ctx, ObGroupByPushdownContext &ctx);

  /**
   * @brief 重写 GroupBy 节点
   * @param groupby GroupBy 节点
   * @param ctx 上下文
   * @param result 结果
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int rewrite_groupby_node(ObLogGroupBy *groupby, ObGroupByPushdownContext *ctx, ObGroupByPushdownResult *result);

  /**
   * @brief 检查 GroupBy 是否可以通过 JOIN 下推
   * @param join JOIN 节点
   * @param ctx 上下文
   * @param can_push 是否可以下推
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int compute_push_through_join_params(ObLogJoin *join,
                                       ObGroupByPushdownContext *ctx,
                                       ObGroupByPushdownContext &left_ctx,
                                       ObGroupByPushdownContext &right_ctx,
                                       bool &can_push_to_left,
                                       bool &can_push_to_right);

  int check_is_simple_mask_aggr_expr(ObRawExpr *aggr_expr,
                                     ObRawExpr *&mask_expr,
                                     ObRawExpr *&param_expr,
                                     bool &is_mask_aggr_expr);

  /**
   * @brief 将单个 groupby 表达式分配到左右子节点
   * @param groupby_expr 需要分配的 groupby 表达式
   * @param left_child 左子节点
   * @param right_child 右子节点
   * @param left_ctx 左子节点上下文
   * @param right_ctx 右子节点上下文
   * @param can_push 是否可以下推
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int distribute_groupby_expr(ObRawExpr *groupby_expr,
                              ObLogicalOperator *left_child,
                              ObLogicalOperator *right_child,
                              ObGroupByPushdownContext &left_ctx,
                              ObGroupByPushdownContext &right_ctx,
                              bool &can_push);

  /**
   * @brief 为 mask aggr 生成新的参数聚合表达式
   * @param aggr_type 聚合函数类型（只支持 T_FUN_SUM, T_FUN_COUNT, T_FUN_MIN, T_FUN_MAX）
   * @param param_expr 参数表达式
   * @param new_param_expr 出参，生成的新聚合表达式
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int generate_mask_aggr_param_expr(ObItemType aggr_type, ObRawExpr *param_expr, ObAggFunRawExpr *&new_param_expr);

  /**
   * @brief 将单个聚合表达式分配到左右子节点
   * @param aggr_expr 需要分配的聚合表达式
   * @param left_child 左子节点
   * @param right_child 右子节点
   * @param left_ctx 左子节点上下文
   * @param right_ctx 右子节点上下文
   * @param is_from_single_side 是否来自单侧（左或右子节点）
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int distribute_aggr_expr(ObRawExpr *aggr_expr,
                           ObLogicalOperator *left_child,
                           ObLogicalOperator *right_child,
                           ObGroupByPushdownContext &left_ctx,
                           ObGroupByPushdownContext &right_ctx,
                           bool &is_from_single_side);

  /**
   * @brief 尝试将聚合表达式作为 mask aggr 下推
   * @param aggr_expr 聚合表达式
   * @param left_child 左子节点
   * @param right_child 右子节点
   * @param left_ctx 左子节点上下文
   * @param right_ctx 右子节点上下文
   * @param ctx 父上下文（用于存储 mask_aggr_infos_）
   * @param can_push 是否可以下推
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int try_pushdown_as_mask_aggr_expr(ObRawExpr *aggr_expr,
                                      ObLogicalOperator *left_child,
                                      ObLogicalOperator *right_child,
                                      ObGroupByPushdownContext &left_ctx,
                                      ObGroupByPushdownContext &right_ctx,
                                      ObGroupByPushdownContext *ctx,
                                      bool &can_push);

  /**
   * @brief 分配 mask aggr 的参数表达式（包含生成和分配）
   * @param aggr_expr 原始的 mask aggr 表达式
   * @param param_expr 参数表达式
   * @param left_child 左子节点
   * @param right_child 右子节点
   * @param left_ctx 左子节点上下文
   * @param right_ctx 右子节点上下文
   * @param ctx 父上下文（用于存储 mask_aggr_infos_）
   * @param can_push 是否可以下推
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int distribute_mask_aggr_param_expr(ObRawExpr *aggr_expr,
                                      ObRawExpr *param_expr,
                                      ObLogicalOperator *left_child,
                                      ObLogicalOperator *right_child,
                                      ObGroupByPushdownContext &left_ctx,
                                      ObGroupByPushdownContext &right_ctx,
                                      ObGroupByPushdownContext *ctx,
                                      bool &can_push);

  /**
   * @brief 尝试将聚合表达式作为 mask aggr 重写
   * @param aggr_expr 聚合表达式
   * @param ctx 上下文（包含 mask_aggr_infos_）
   * @param from_exprs 源表达式数组
   * @param to_exprs 目标表达式数组
   * @param result 结果（用于存储重写后的表达式）
   * @param rewrite_happened 是否成功重写
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int try_rewrite_as_mask_aggr_expr(ObRawExpr *aggr_expr,
                                     ObGroupByPushdownContext *ctx,
                                     const common::ObIArray<ObRawExpr *> &from_exprs,
                                     const common::ObIArray<ObRawExpr *> &to_exprs,
                                     ObGroupByPushdownResult *result,
                                     bool &rewrite_happened);

  /**
   * @brief 将 groupby 表达式分配到左右子节点
   * @param groupby_exprs 需要分配的 groupby 表达式数组
   * @param left_child 左子节点
   * @param right_child 右子节点
   * @param left_ctx 左子节点上下文
   * @param right_ctx 右子节点上下文
   * @param can_push 是否可以下推
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int distribute_groupby_exprs(const common::ObIArray<ObRawExpr *> &groupby_exprs,
                               ObLogicalOperator *left_child,
                               ObLogicalOperator *right_child,
                               ObGroupByPushdownContext &left_ctx,
                               ObGroupByPushdownContext &right_ctx,
                               bool &can_push);

  /**
   * @brief 获取 join 键
   * @param join JOIN 节点
   * @param left_join_keys 左键
   * @param right_join_keys 右键
   * @param is_valid 连接是否能下推，如果为false，则不能下推Groupby过连接
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int get_join_keys(ObLogJoin *join,
                    ObSEArray<ObRawExpr *, 4> &left_join_keys,
                    ObSEArray<ObRawExpr *, 4> &right_join_keys,
                    bool &is_valid);

  /**
   * @brief 替换 join 节点的 exprs
   * @param op 当前节点
   * @param left_ctx 左支上下文
   * @param left_result 左支结果
   * @param right_ctx 右支上下文
   * @param right_result 右支结果
   * @param ctx 上下文
   * @param result 结果
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int rewrite_join_node(ObLogJoin *join,
                        bool can_push_to_left,
                        bool can_push_to_right,
                        ObGroupByPushdownContext *left_ctx,
                        ObGroupByPushdownResult *left_result,
                        ObGroupByPushdownContext *right_ctx,
                        ObGroupByPushdownResult *right_result,
                        ObGroupByPushdownContext *parent_ctx,
                        ObGroupByPushdownResult *parent_result);

  /**
   * @brief 替换 join 节点的 aggr 表达式
   * @param aggr_exprs 需要替换的 aggr 表达式
   * @param source_ctx 源上下文
   * @param source_result 源结果
   * @param other_ctx 另一支的上下文
   * @param other_result 另一支的结果
   * @param new_aggr_exprs 出参，新的 aggr 表达式
   * @return OB_SUCCESS 成功，其他值表示失败
   * @note TODO tuliwei.tlw: 解释一下什么是source和other
   */
  int rewrite_join_aggr_exprs(ObSEArray<ObRawExpr *, 4> &aggr_exprs,
                              ObGroupByPushdownContext &source_ctx,
                              ObGroupByPushdownResult &source_result,
                              ObGroupByPushdownContext &other_ctx,
                              ObGroupByPushdownResult &other_result,
                              ObIArray<ObRawExpr *> &new_aggr_exprs);

  int build_count_expr_for_join(ObGroupByPushdownContext &ctx,
                                ObGroupByPushdownContext &left_ctx,
                                ObGroupByPushdownResult &left_result,
                                ObGroupByPushdownContext &right_ctx,
                                ObGroupByPushdownResult &right_result,
                                ObRawExpr *&count_expr);

  /**
   * @brief 检查是否可以通过 Exchange 下推
   * @param exchange Exchange 节点
   * @param ctx 上下文
   * @param can_push 是否可以下推
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int can_push_through_exchange(ObLogExchange *exchange, ObGroupByPushdownContext *ctx, bool &can_push);

  int set_context_flags_for_exchange(ObLogExchange *exchange, ObGroupByPushdownContext *ctx);

  /**
   * @brief 尝试重写Set节点的所有子节点
   * @param set Set节点
   * @param ctx 上下文
   * @param child_contexts 子节点上下文数组
   * @param child_results 子节点结果数组
   * @param has_pushed_down 是否有子节点被下推
   * @param pushdown_child_stmt 被下推的子节点stmt
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int try_rewrite_set_children(ObLogSet *set,
                               ObGroupByPushdownContext *ctx,
                               ObFixedArray<ObGroupByPushdownContext, ObIAllocator> &child_contexts,
                               ObFixedArray<ObGroupByPushdownResult, ObIAllocator> &child_results,
                               bool &has_pushed_down,
                               const ObSelectStmt *&pushdown_child_stmt);

  /**
   * @brief 为Set节点的子节点添加投影
   * @param set Set节点
   * @param child_contexts 子节点上下文数组
   * @param child_results 子节点结果数组
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int add_projection_for_set_children(ObLogSet *set,
                                     ObFixedArray<ObGroupByPushdownContext, ObIAllocator> &child_contexts,
                                     ObFixedArray<ObGroupByPushdownResult, ObIAllocator> &child_results);

  /**
   * @brief 重写Set节点，添加新的聚合表达式
   * @param set Set节点
   * @param ctx 上下文
   * @param pushdown_child_stmt 被下推的子节点stmt
   * @param result 结果
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int rewrite_set_node(ObLogSet *set,
                      ObGroupByPushdownContext *ctx,
                      const ObSelectStmt *pushdown_child_stmt,
                      ObGroupByPushdownResult *result);

  /**
   * @brief 构建新的聚合表达式，供添加新Groupby算子使用
   * @param op 当前节点
   * @param ctx 上下文
   * @param result 结果
   * @param new_aggr_exprs 出参，构建的新的聚合表达式
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int get_aggr_item_for_new_groupby(ObLogicalOperator *op,
                                    ObGroupByPushdownContext *ctx,
                                    ObGroupByPushdownResult *result,
                                    ObIArray<ObAggFunRawExpr *> &aggr_items);

  /**
   * @brief 执行放置groupby
   * @param op 当前节点
   * @param ctx 上下文
   * @param result 结果
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int do_place_groupby(ObLogicalOperator *op,
                       ObGroupByPushdownContext *ctx,
                       ObGroupByPushdownResult *result,
                       double ndv,
                       bool need_reshuffle);

  /**
   * @brief 尝试放置groupby
   * @param op 当前节点
   * @param ctx 上下文
   * @param result 结果
   * @return OB_SUCCESS 成功，其他值表示失败
   * @note 放置后需要更新parent的child
   */
  int try_place_groupby(ObLogicalOperator *op, ObGroupByPushdownContext *ctx, ObGroupByPushdownResult *result);

  // int check_is_sharding_match(ObLogicalOperator *op, ObIArray<ObRawExpr *> &groupby_exprs, bool &is_sharding_match);

public:
  bool aggr_need_count(ObRawExpr *aggr_expr)
  {
    if (OB_NOT_NULL(aggr_expr)) {
      return aggr_expr->get_expr_type() == T_FUN_COUNT || aggr_expr->get_expr_type() == T_FUN_SUM;
    }
    return false;
  }

  /**
   * @brief 为 COUNT 聚合函数构建 CASE WHEN 表达式 "case when param_expr is not null then 1 else 0"
   * @param param_expr COUNT 聚合函数的参数表达式
   * @param case_when_expr 出参，构建的 CASE WHEN 表达式
   * @return OB_SUCCESS 成功，其他值表示失败
   */
  int build_case_when_expr_for_count(ObRawExpr *param_expr, ObRawExpr *&case_when_expr);

private:
  ObOptimizerContext &opt_ctx_;
  bool transform_happened_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_PUSH_DOWN_GROUPBY_RULE_H
